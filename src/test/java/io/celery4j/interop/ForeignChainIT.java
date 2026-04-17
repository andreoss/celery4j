/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.interop;

import io.celery4j.protocol.Message;
import io.celery4j.result.Backend;
import io.celery4j.result.FakeBackend;
import io.celery4j.result.RedisBackend;
import io.celery4j.result.TaskResult;
import io.celery4j.transport.Broker;
import io.celery4j.transport.RedisBroker;
import io.celery4j.worker.Registry;
import io.celery4j.worker.Worker;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import redis.clients.jedis.JedisPool;

/**
 * Live test case for a chain the framework handed over and this library
 * carried to its end.
 *
 * <p>Reading the work-flow fields is checked elsewhere. What matters to
 * anybody deploying this is whether a chain written by the framework reaches
 * its last task, with each result put in front of the next call, and whether
 * the framework's own client can read the end of it.</p>
 *
 * @since 1.0.0
 */
@Tag("live")
@Testcontainers
final class ForeignChainIT {

    /**
     * Port the store listens on inside its container.
     */
    private static final int PORT = 6379;

    /**
     * How long a receive waits.
     */
    private static final Duration WAIT = Duration.ofSeconds(10L);

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
            .withExposedPorts(ForeignChainIT.PORT)
            .withNetwork(ForeignChainIT.NETWORK)
            .withNetworkAliases("store");

    /**
     * The foreign side, holding the producer and the client these cases run.
     */
    @Container
    private static final GenericContainer<?> FOREIGN = ForeignChainIT.foreign();

    @Test
    void runsBothLinksOfAChainTheFrameworkSent() {
        Assertions.assertEquals(2, ForeignChainIT.carried().size());
    }

    @Test
    void putsTheResultOfOneLinkInFrontOfTheNext() {
        Assertions.assertEquals(
            Optional.of(List.of(List.of(1), 2)),
            ForeignChainIT.carried().get(1).value()
        );
    }

    @Test
    void leavesNothingOnTheQueueOnceTheChainIsOver() {
        Assertions.assertEquals(Optional.empty(), ForeignChainIT.left());
    }

    @Test
    void letsTheFrameworkReadTheEndOfTheChain() {
        Assertions.assertTrue(ForeignChainIT.ending().contains("STATE SUCCESS"));
    }

    @Test
    void keepsTheForeignSideRunning() {
        Assertions.assertTrue(ForeignChainIT.FOREIGN.isRunning());
    }

    private static String ending() {
        final List<TaskResult> done = ForeignChainIT.stored(ForeignChainIT.queue());
        return ForeignChainIT.read(done.get(done.size() - 1).id());
    }

    private static Optional<Message> left() {
        final String queue = ForeignChainIT.queue();
        ForeignChainIT.carried(queue);
        try (Broker broker = new RedisBroker(ForeignChainIT.pool())) {
            return broker.receive(queue, ForeignChainIT.WAIT);
        }
    }

    private static List<TaskResult> carried() {
        return ForeignChainIT.carried(ForeignChainIT.queue());
    }

    private static List<TaskResult> carried(final String queue) {
        ForeignChainIT.ask(queue);
        try (Broker broker = new RedisBroker(ForeignChainIT.pool())) {
            return ForeignChainIT.worker(broker, new FakeBackend(new ConcurrentHashMap<>()))
                .some(queue, ForeignChainIT.WAIT, 2);
        }
    }

    private static List<TaskResult> stored(final String queue) {
        ForeignChainIT.ask(queue);
        try (
            Broker broker = new RedisBroker(ForeignChainIT.pool());
            Backend backend = new RedisBackend(ForeignChainIT.pool())
        ) {
            return ForeignChainIT.worker(broker, backend).some(queue, ForeignChainIT.WAIT, 2);
        }
    }

    private static Worker worker(final Broker broker, final Backend backend) {
        return new Worker(
            broker,
            backend,
            new Registry(Map.of("proj.java.echo", task -> task.args()))
        );
    }

    private static void ask(final String queue) {
        try {
            ForeignChainIT.FOREIGN.execInContainer("python", "workflow.py", "chain", queue);
        } catch (final IOException ex) {
            throw new IllegalStateException("the foreign producer could not be run", ex);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("running the foreign producer was interrupted", ex);
        }
    }

    private static String read(final String id) {
        try {
            final ExecResult said =
                ForeignChainIT.FOREIGN.execInContainer("python", "read.py", id);
            return String.format("%s%s", said.getStdout(), said.getStderr());
        } catch (final IOException ex) {
            throw new IllegalStateException("the foreign client could not be run", ex);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("running the foreign client was interrupted", ex);
        }
    }

    private static String queue() {
        return String.format("chained-%s", UUID.randomUUID());
    }

    private static GenericContainer<?> foreign() {
        return new GenericContainer<>(ForeignChainIT.image())
            .withNetwork(ForeignChainIT.NETWORK)
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
            .withFileFromClasspath("workflow.py", "live/workflow.py")
            .withFileFromClasspath("header.py", "live/header.py");
    }

    private static JedisPool pool() {
        return new JedisPool(
            ForeignChainIT.STORE.getHost(), ForeignChainIT.STORE.getMappedPort(ForeignChainIT.PORT)
        );
    }
}
