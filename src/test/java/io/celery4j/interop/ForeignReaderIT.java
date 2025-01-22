/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.interop;

import io.celery4j.protocol.ProtocolV2;
import io.celery4j.protocol.Task;
import io.celery4j.result.Backend;
import io.celery4j.result.RedisBackend;
import io.celery4j.transport.Broker;
import io.celery4j.transport.RedisBroker;
import io.celery4j.worker.Job;
import io.celery4j.worker.Registry;
import io.celery4j.worker.Worker;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Testcontainers;
import redis.clients.jedis.JedisPool;

/**
 * Live test case for the direction nothing had checked: the framework's own
 * client reading a result that a worker of this library wrote.
 *
 * <p>Every other case reads what the other side wrote. A result whose shape
 * only this library understands would have passed all of them, and would have
 * been useless to anyone waiting on the other side.</p>
 *
 * @since 0.3.0
 */
@Tag("live")
@Testcontainers
final class ForeignReaderIT {

    /**
     * Port the store listens on inside its container.
     */
    private static final int PORT = 6379;

    /**
     * Name of the task these cases run.
     */
    private static final String NAME = "proj.java.sum";

    /**
     * How long a receive waits.
     */
    private static final Duration WAIT = Duration.ofSeconds(5L);

    /**
     * Network the store and the reader meet on.
     */
    private static final Network NETWORK = Network.newNetwork();

    /**
     * The store both sides use.
     */
    @org.testcontainers.junit.jupiter.Container
    private static final GenericContainer<?> STORE =
        new GenericContainer<>("docker.io/library/redis:7-alpine")
            .withExposedPorts(ForeignReaderIT.PORT)
            .withNetwork(ForeignReaderIT.NETWORK)
            .withNetworkAliases("store");

    /**
     * The foreign side, holding the client these cases run.
     */
    @org.testcontainers.junit.jupiter.Container
    private static final GenericContainer<?> FOREIGN = ForeignReaderIT.foreign();

    @Test
    void handsTheValueToTheOtherSide() {
        Assertions.assertTrue(
            ForeignReaderIT.read(ForeignReaderIT.ran(task -> 5)).contains("VALUE 5")
        );
    }

    @Test
    void handsTheStateToTheOtherSide() {
        Assertions.assertTrue(
            ForeignReaderIT.read(ForeignReaderIT.ran(task -> 5)).contains("STATE SUCCESS")
        );
    }

    @Test
    void handsAListToTheOtherSide() {
        Assertions.assertTrue(
            ForeignReaderIT.read(ForeignReaderIT.ran(task -> List.of(1, 2)))
                .contains("VALUE [1, 2]")
        );
    }

    @Test
    void makesTheOtherSideRaiseOnAFailure() {
        Assertions.assertTrue(
            ForeignReaderIT.read(ForeignReaderIT.ran(ForeignReaderIT.raising()))
                .contains("RAISED")
        );
    }

    @Test
    void tellsTheOtherSideWhatTheFailureSaid() {
        Assertions.assertTrue(
            ForeignReaderIT.read(ForeignReaderIT.ran(ForeignReaderIT.raising())).contains("boom")
        );
    }

    @Test
    void handsTheFailedStateToTheOtherSide() {
        Assertions.assertTrue(
            ForeignReaderIT.read(ForeignReaderIT.ran(ForeignReaderIT.raising()))
                .contains("STATE FAILURE")
        );
    }

    @Test
    void keepsTheForeignSideRunning() {
        Assertions.assertTrue(ForeignReaderIT.FOREIGN.isRunning());
    }

    private static Job raising() {
        return task -> {
            throw new IllegalStateException("boom");
        };
    }

    private static String ran(final Job job) {
        final String id = UUID.randomUUID().toString();
        final String queue = String.format("reader-%s", UUID.randomUUID());
        try (
            Broker broker = new RedisBroker(ForeignReaderIT.pool());
            Backend backend = new RedisBackend(ForeignReaderIT.pool())
        ) {
            broker.send(
                new ProtocolV2().message(
                    new Task(id, ForeignReaderIT.NAME, List.of(2, 3), Map.of()), queue
                ),
                queue
            );
            final boolean ran = new Worker(
                broker, backend, new Registry(Map.of(ForeignReaderIT.NAME, job))
            ).once(queue, ForeignReaderIT.WAIT).isPresent();
            if (!ran) {
                throw new IllegalStateException("the task was never run");
            }
        }
        return id;
    }

    private static String read(final String id) {
        try {
            final Container.ExecResult said =
                ForeignReaderIT.FOREIGN.execInContainer("python", "read.py", id);
            return String.format("%s%s", said.getStdout(), said.getStderr());
        } catch (final IOException ex) {
            throw new IllegalStateException("the foreign client could not be run", ex);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("running the foreign client was interrupted", ex);
        }
    }

    private static GenericContainer<?> foreign() {
        return new GenericContainer<>(ForeignReaderIT.image())
            .withNetwork(ForeignReaderIT.NETWORK)
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
            ForeignReaderIT.STORE.getHost(),
            ForeignReaderIT.STORE.getMappedPort(ForeignReaderIT.PORT)
        );
    }
}
