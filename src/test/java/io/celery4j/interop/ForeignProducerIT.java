/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.interop;

import io.celery4j.protocol.JsonMessageCodec;
import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageHeaders;
import io.celery4j.protocol.Protocols;
import io.celery4j.result.Backend;
import io.celery4j.result.RedisBackend;
import io.celery4j.result.State;
import io.celery4j.result.TaskResult;
import io.celery4j.transport.Broker;
import io.celery4j.transport.RedisBroker;
import io.celery4j.worker.Registry;
import io.celery4j.worker.Worker;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Live test case with the original implementation on the producing side: it
 * writes the messages, this library reads and runs them.
 *
 * <p>Every message read here is written out under the build directory, so that
 * a capture can be committed as a fixture and checked without a container
 * runtime afterwards.</p>
 *
 * @since 0.1.1
 */
@Tag("live")
@Testcontainers
final class ForeignProducerIT {

    /**
     * Port the store listens on inside its container.
     */
    private static final int PORT = 6379;

    /**
     * Queue the foreign producer writes to.
     */
    private static final String QUEUE = "fromforeign";

    /**
     * Name of the task the foreign producer asks for.
     */
    private static final String ECHO = "proj.java.echo";

    /**
     * How long to wait for a message the foreign side wrote.
     */
    private static final Duration PATIENCE = Duration.ofSeconds(60L);

    /**
     * Where a captured message is written out.
     */
    private static final Path CAPTURED = Path.of("target", "captured");

    /**
     * Network the store and the producer meet on.
     */
    private static final Network NETWORK = Network.newNetwork();

    /**
     * The store both sides use.
     */
    @Container
    private static final GenericContainer<?> STORE =
        new GenericContainer<>("docker.io/library/redis:7-alpine")
            .withExposedPorts(ForeignProducerIT.PORT)
            .withNetwork(ForeignProducerIT.NETWORK)
            .withNetworkAliases("store");

    /**
     * The foreign side, holding the producer this suite runs.
     */
    @Container
    private static final GenericContainer<?> FOREIGN = ForeignProducerIT.foreign();

    @Test
    void readsTheTaskNameAForeignProducerWrote() {
        Assertions.assertEquals(
            ForeignProducerIT.ECHO,
            ForeignProducerIT.produced(UUID.randomUUID().toString()).task()
        );
    }

    @Test
    void readsTheIdentifierAForeignProducerWrote() {
        final String id = UUID.randomUUID().toString();
        Assertions.assertEquals(id, ForeignProducerIT.produced(id).id());
    }

    @Test
    void readsThePositionalArgumentsAForeignProducerWrote() {
        Assertions.assertEquals(
            List.of(2, 3),
            new Protocols()
                .task(ForeignProducerIT.produced(UUID.randomUUID().toString()))
                .args()
        );
    }

    @Test
    void readsTheKeywordArgumentsAForeignProducerWrote() {
        Assertions.assertEquals(
            Map.of("debug", true),
            new Protocols()
                .task(ForeignProducerIT.produced(UUID.randomUUID().toString()))
                .kwargs()
        );
    }

    @Test
    void readsTheLanguageAForeignProducerNamed() {
        Assertions.assertEquals(
            Optional.of("py"),
            ForeignProducerIT.produced(UUID.randomUUID().toString())
                .headers()
                .text(MessageHeaders.LANG)
        );
    }

    @Test
    void runsATaskAForeignProducerAskedFor() {
        ForeignProducerIT.ask(UUID.randomUUID().toString());
        try (
            Broker broker = new RedisBroker(ForeignProducerIT.pool());
            Backend backend = new RedisBackend(ForeignProducerIT.pool())
        ) {
            Assertions.assertEquals(
                Optional.of(5),
                ForeignProducerIT.adding(broker, backend)
                    .once(ForeignProducerIT.QUEUE, ForeignProducerIT.PATIENCE)
                    .orElseThrow()
                    .value()
            );
        }
    }

    @Test
    void publishesAResultTheForeignSideCanFind() {
        final String id = UUID.randomUUID().toString();
        ForeignProducerIT.ask(id);
        try (
            Broker broker = new RedisBroker(ForeignProducerIT.pool());
            Backend backend = new RedisBackend(ForeignProducerIT.pool())
        ) {
            new Worker(
                broker,
                backend,
                new Registry(Map.of(ForeignProducerIT.ECHO, task -> 5))
            ).once(ForeignProducerIT.QUEUE, ForeignProducerIT.PATIENCE);
            Assertions.assertEquals(
                new State(State.SUCCESS),
                backend.of(id).map(TaskResult::state).orElseThrow()
            );
        }
    }

    private static Worker adding(final Broker broker, final Backend backend) {
        return new Worker(
            broker,
            backend,
            new Registry(
                Map.of(
                    ForeignProducerIT.ECHO,
                    task -> task.args().stream().mapToInt(arg -> (Integer) arg).sum()
                )
            )
        );
    }

    private static Message produced(final String id) {
        ForeignProducerIT.ask(id);
        final byte[] raw = ForeignProducerIT.raw();
        ForeignProducerIT.capture(raw);
        return new JsonMessageCodec().decode(raw);
    }

    private static byte[] raw() {
        try (Jedis jedis = ForeignProducerIT.pool().getResource()) {
            return jedis.brpop(
                (double) ForeignProducerIT.PATIENCE.toSeconds(),
                ForeignProducerIT.QUEUE.getBytes(StandardCharsets.UTF_8)
            ).get(1);
        }
    }

    private static void ask(final String id) {
        try {
            ForeignProducerIT.FOREIGN.execInContainer(
                "python", "producer.py", id, ForeignProducerIT.ECHO, ForeignProducerIT.QUEUE
            );
        } catch (final IOException ex) {
            throw new IllegalStateException("the foreign producer could not be run", ex);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("running the foreign producer was interrupted", ex);
        }
    }

    private static void capture(final byte[] raw) {
        try {
            Files.createDirectories(ForeignProducerIT.CAPTURED);
            Files.write(ForeignProducerIT.CAPTURED.resolve("foreign-message.json"), raw);
        } catch (final IOException ex) {
            throw new IllegalStateException("the message could not be written out", ex);
        }
    }

    private static GenericContainer<?> foreign() {
        return new GenericContainer<>(ForeignProducerIT.image())
            .withNetwork(ForeignProducerIT.NETWORK)
            .withEnv("BROKER_URL", "redis://store:6379/0")
            .withStartupTimeout(Duration.ofMinutes(5L));
    }

    private static ImageFromDockerfile image() {
        return new ImageFromDockerfile()
            .withFileFromClasspath("Dockerfile", "live/Dockerfile")
            .withFileFromClasspath("tasks.py", "live/tasks.py")
            .withFileFromClasspath("producer.py", "live/producer.py")
            .withFileFromClasspath("schedule.py", "live/schedule.py");
    }

    private static JedisPool pool() {
        return new JedisPool(
            ForeignProducerIT.STORE.getHost(),
            ForeignProducerIT.STORE.getMappedPort(ForeignProducerIT.PORT)
        );
    }
}
