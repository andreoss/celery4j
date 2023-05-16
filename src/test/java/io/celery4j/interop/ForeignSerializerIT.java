/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.interop;

import io.celery4j.protocol.BodyCodecs;
import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageProperties;
import io.celery4j.protocol.ProtocolException;
import io.celery4j.result.FakeBackend;
import io.celery4j.transport.Broker;
import io.celery4j.transport.RedisBroker;
import io.celery4j.worker.Registry;
import io.celery4j.worker.Worker;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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
 * Live test case for a body this library cannot read.
 *
 * <p>The other side can write a body in forms this library refuses. What it
 * must not do is read one of them as if it were JSON: a task run with
 * parameters that were never sent is worse than a task not run at all.</p>
 *
 * @since 0.2.1
 */
@Tag("live")
@Testcontainers
final class ForeignSerializerIT {

    /**
     * Port the store listens on inside its container.
     */
    private static final int PORT = 6379;

    /**
     * Name of the task the foreign producer asks for.
     */
    private static final String ECHO = "proj.java.echo";

    /**
     * How long a receive waits.
     */
    private static final Duration WAIT = Duration.ofSeconds(5L);

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
            .withExposedPorts(ForeignSerializerIT.PORT)
            .withNetwork(ForeignSerializerIT.NETWORK)
            .withNetworkAliases("store");

    /**
     * The foreign side, holding the producer this suite runs.
     */
    @Container
    private static final GenericContainer<?> FOREIGN = ForeignSerializerIT.foreign();

    @Test
    void namesTheFormAMessageWasWrittenIn() {
        Assertions.assertEquals(
            Optional.of("application/x-yaml"),
            ForeignSerializerIT.written("yaml").properties().text(MessageProperties.TYPE)
        );
    }

    @Test
    void refusesAFormItDoesNotKnow() {
        final Message message = ForeignSerializerIT.written("yaml");
        final BodyCodecs codecs = new BodyCodecs();
        Assertions.assertThrows(ProtocolException.class, () -> codecs.of(message));
    }

    @Test
    void readsTheHeadersOfAMessageItCannotRead() {
        Assertions.assertEquals(
            ForeignSerializerIT.ECHO, ForeignSerializerIT.written("yaml").task()
        );
    }

    @Test
    void refusesToRunATaskItCannotRead() {
        final String queue = ForeignSerializerIT.queue();
        ForeignSerializerIT.ask(queue, "yaml");
        try (Broker broker = new RedisBroker(ForeignSerializerIT.pool())) {
            final Worker worker = new Worker(
                broker,
                new FakeBackend(new ConcurrentHashMap<>()),
                new Registry(Map.of(ForeignSerializerIT.ECHO, task -> 4))
            );
            Assertions.assertThrows(
                ProtocolException.class, () -> worker.once(queue, ForeignSerializerIT.WAIT)
            );
        }
    }

    @Test
    void readsAFormItKnows() {
        Assertions.assertEquals(
            Optional.of(MessageProperties.JSON),
            ForeignSerializerIT.written("json").properties().text(MessageProperties.TYPE)
        );
    }

    @Test
    void keepsTheForeignSideRunning() {
        Assertions.assertTrue(ForeignSerializerIT.FOREIGN.isRunning());
    }

    private static Message written(final String form) {
        final String queue = ForeignSerializerIT.queue();
        ForeignSerializerIT.ask(queue, form);
        try (Broker broker = new RedisBroker(ForeignSerializerIT.pool())) {
            return broker.receive(queue, ForeignSerializerIT.WAIT).orElseThrow();
        }
    }

    private static void ask(final String queue, final String form) {
        try {
            ForeignSerializerIT.FOREIGN.execInContainer(
                "python",
                "serialize.py",
                UUID.randomUUID().toString(),
                ForeignSerializerIT.ECHO,
                queue,
                form
            );
        } catch (final IOException ex) {
            throw new IllegalStateException("the foreign producer could not be run", ex);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("running the foreign producer was interrupted", ex);
        }
    }

    private static String queue() {
        return String.format("written-%s", UUID.randomUUID());
    }

    private static GenericContainer<?> foreign() {
        return new GenericContainer<>(ForeignSerializerIT.image())
            .withNetwork(ForeignSerializerIT.NETWORK)
            .withEnv("BROKER_URL", "redis://store:6379/0")
            .withStartupTimeout(Duration.ofMinutes(5L));
    }

    private static ImageFromDockerfile image() {
        return new ImageFromDockerfile()
            .withFileFromClasspath("Dockerfile", "live/Dockerfile")
            .withFileFromClasspath("tasks.py", "live/tasks.py")
            .withFileFromClasspath("producer.py", "live/producer.py")
            .withFileFromClasspath("schedule.py", "live/schedule.py")
            .withFileFromClasspath("serialize.py", "live/serialize.py")
            .withFileFromClasspath("read.py", "live/read.py")
            .withFileFromClasspath("workflow.py", "live/workflow.py");
    }

    private static JedisPool pool() {
        return new JedisPool(
            ForeignSerializerIT.STORE.getHost(),
            ForeignSerializerIT.STORE.getMappedPort(ForeignSerializerIT.PORT)
        );
    }
}
