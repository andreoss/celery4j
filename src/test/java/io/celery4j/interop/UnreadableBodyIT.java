/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.interop;

import io.celery4j.protocol.JsonMessageCodec;
import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageHeaders;
import io.celery4j.protocol.ProtocolException;
import io.celery4j.result.Backend;
import io.celery4j.result.FakeBackend;
import io.celery4j.result.RedisBackend;
import io.celery4j.result.Results;
import io.celery4j.result.State;
import io.celery4j.transport.Broker;
import io.celery4j.transport.Queues;
import io.celery4j.transport.RedisBroker;
import io.celery4j.worker.Registry;
import io.celery4j.worker.Worker;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import redis.clients.jedis.JedisPool;

/**
 * Live test case for a body the framework wrote in a form this library does
 * not carry.
 *
 * <p>Refusing such a message is checked where the codecs are, on a body this
 * library never took off a queue. What nobody has shown is what happens to it
 * afterwards: whether a worker that cannot read it swallows it, and whether a
 * consumer that can read it still gets its chance. The consumer here is
 * started only once this library has refused and let go, so there is never a
 * moment where two of them want the same message.</p>
 *
 * @since 1.0.0
 */
@Tag("live")
@Testcontainers
final class UnreadableBodyIT {

    /**
     * Port the store listens on inside its container.
     */
    private static final int PORT = 6379;

    /**
     * Queue the strange message travels on.
     */
    private static final String QUEUE = String.format("strange-%s", UUID.randomUUID());

    /**
     * Queue the message meant for the other side travels on.
     */
    private static final String HANDED = String.format("handed-%s", UUID.randomUUID());

    /**
     * Name of the list a worker holds what it took on.
     */
    private static final String HELD = String.format("held-%s", UUID.randomUUID());

    /**
     * How long a receive waits.
     */
    private static final Duration WAIT = Duration.ofSeconds(5L);

    /**
     * How long a task may take to be run by the other side.
     */
    private static final Duration PATIENCE = Duration.ofSeconds(60L);

    /**
     * Network the store and the foreign side meet on.
     */
    private static final Network NETWORK = Network.newNetwork();

    /**
     * The store both sides use.
     */
    @Container
    private static final GenericContainer<?> STORE =
        new GenericContainer<>("docker.io/library/redis:7-alpine")
            .withExposedPorts(UnreadableBodyIT.PORT)
            .withNetwork(UnreadableBodyIT.NETWORK)
            .withNetworkAliases("store");

    /**
     * The foreign side that writes the message, reading a queue of its own
     * that nothing is ever written to.
     */
    @Container
    private static final GenericContainer<?> WRITER = UnreadableBodyIT.foreign(
        String.format("idle-%s", UUID.randomUUID())
    );

    /**
     * The foreign side that can read what this library cannot, started by
     * hand once this library has let the message go.
     */
    private static final GenericContainer<?> READER =
        UnreadableBodyIT.foreign(UnreadableBodyIT.HANDED);

    @AfterAll
    static void stop() {
        if (UnreadableBodyIT.READER.isRunning()) {
            UnreadableBodyIT.READER.stop();
        }
    }

    @Test
    void refusesABodyTheFrameworkWroteInAFormItDoesNotCarry() {
        Assertions.assertThrows(
            ProtocolException.class, () -> UnreadableBodyIT.refused(UnreadableBodyIT.worker())
        );
    }

    @Test
    void namesTheFormItCannotRead() {
        Assertions.assertTrue(
            Assertions.assertThrows(
                ProtocolException.class,
                () -> UnreadableBodyIT.refused(UnreadableBodyIT.worker())
            ).getMessage().contains("yaml")
        );
    }

    @Test
    void readsTheHeadersOfAMessageItCannotRead() {
        UnreadableBodyIT.written(UnreadableBodyIT.QUEUE, UUID.randomUUID().toString());
        try (Broker broker = UnreadableBodyIT.broker()) {
            final Optional<Message> message =
                broker.receive(UnreadableBodyIT.QUEUE, UnreadableBodyIT.WAIT);
            broker.restore();
            Assertions.assertEquals(
                Optional.of("proj.tasks.add"),
                message.orElseThrow().headers().text(MessageHeaders.TASK)
            );
        }
    }

    @Test
    void letsAConsumerThatCanReadItFinishTheTask() {
        final String id = UUID.randomUUID().toString();
        UnreadableBodyIT.written(UnreadableBodyIT.HANDED, id);
        try (Broker broker = UnreadableBodyIT.broker()) {
            Assertions.assertThrows(
                ProtocolException.class,
                () -> new Worker(
                    broker,
                    new FakeBackend(new ConcurrentHashMap<>()),
                    new Registry(Map.of())
                ).once(UnreadableBodyIT.HANDED, UnreadableBodyIT.WAIT)
            );
            broker.restore();
        }
        UnreadableBodyIT.reading();
        try (Backend backend = new RedisBackend(UnreadableBodyIT.pool())) {
            Assertions.assertEquals(
                new State(State.SUCCESS),
                new Results(backend, Duration.ofMillis(200L))
                    .await(id, UnreadableBodyIT.PATIENCE)
                    .orElseThrow()
                    .state()
            );
        }
    }

    @Test
    void keepsTheForeignSideRunning() {
        Assertions.assertTrue(UnreadableBodyIT.WRITER.isRunning());
    }

    private static void refused(final Worker worker) {
        UnreadableBodyIT.written(UnreadableBodyIT.QUEUE, UUID.randomUUID().toString());
        worker.once(UnreadableBodyIT.QUEUE, UnreadableBodyIT.WAIT);
    }

    private static Worker worker() {
        return new Worker(
            UnreadableBodyIT.broker(),
            new FakeBackend(new ConcurrentHashMap<>()),
            new Registry(Map.of())
        );
    }

    private static Broker broker() {
        return new RedisBroker(
            UnreadableBodyIT.pool(),
            new JsonMessageCodec(),
            new Queues(),
            UnreadableBodyIT.HELD
        );
    }

    private static void written(final String queue, final String id) {
        try {
            UnreadableBodyIT.WRITER.execInContainer(
                "python", "serialize.py", id, "proj.tasks.add", queue, "yaml"
            );
        } catch (final IOException ex) {
            throw new IllegalStateException("the foreign producer could not be run", ex);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("running the foreign producer was interrupted", ex);
        }
    }

    private static void reading() {
        if (!UnreadableBodyIT.READER.isRunning()) {
            UnreadableBodyIT.READER.start();
        }
    }

    private static GenericContainer<?> foreign(final String queue) {
        return new GenericContainer<>(UnreadableBodyIT.image())
            .withNetwork(UnreadableBodyIT.NETWORK)
            .withEnv("BROKER_URL", "redis://store:6379/0")
            .withCommand(UnreadableBodyIT.command(queue))
            .withStartupTimeout(Duration.ofMinutes(5L));
    }

    private static String command(final String queue) {
        return String.format(
            "celery -A tasks worker --loglevel=info --concurrency=1 -Q %s", queue
        );
    }

    private static ImageFromDockerfile image() {
        return new ImageFromDockerfile()
            .withFileFromClasspath("Dockerfile", "live/Dockerfile")
            .withFileFromClasspath("tasks.py", "live/tasks.py")
            .withFileFromClasspath("producer.py", "live/producer.py")
            .withFileFromClasspath("schedule.py", "live/schedule.py")
            .withFileFromClasspath("serialize.py", "live/serialize.py")
            .withFileFromClasspath("read.py", "live/read.py")
            .withFileFromClasspath("workflow.py", "live/workflow.py")
            .withFileFromClasspath("header.py", "live/header.py");
    }

    private static JedisPool pool() {
        return new JedisPool(
            UnreadableBodyIT.STORE.getHost(),
            UnreadableBodyIT.STORE.getMappedPort(UnreadableBodyIT.PORT)
        );
    }
}
