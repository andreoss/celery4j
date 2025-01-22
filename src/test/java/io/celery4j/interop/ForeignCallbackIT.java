/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.interop;

import io.celery4j.result.FakeBackend;
import io.celery4j.result.State;
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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import redis.clients.jedis.JedisPool;

/**
 * Live test case for what the framework attaches to a task for success and
 * for failure, run by a worker of this library.
 *
 * <p>A handler attached to a task is the one part of a work-flow a deployment
 * relies on while something is going wrong. Reading the field is worth
 * nothing; what matters is that the handler runs, and that it is handed what
 * the framework would have handed it.</p>
 *
 * @since 1.0.0
 */
@Tag("live")
@Testcontainers
final class ForeignCallbackIT {

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
            .withExposedPorts(ForeignCallbackIT.PORT)
            .withNetwork(ForeignCallbackIT.NETWORK)
            .withNetworkAliases("store");

    /**
     * The foreign side, holding the producer these cases run.
     */
    @Container
    private static final GenericContainer<?> FOREIGN = ForeignCallbackIT.foreign();

    @Test
    void runsWhatTheFrameworkAttachedForSuccess() {
        Assertions.assertEquals(2, ForeignCallbackIT.done("link").size());
    }

    @Test
    void handsTheResultToWhatWasAttachedForSuccess() {
        Assertions.assertEquals(
            Optional.of(List.of(List.of(3), 4)),
            ForeignCallbackIT.done("link").get(1).value()
        );
    }

    @Test
    void runsWhatTheFrameworkAttachedForFailure() {
        Assertions.assertEquals(2, ForeignCallbackIT.done("error").size());
    }

    @Test
    void handsTheTaskThatFailedToWhatWasAttachedForFailure() {
        final List<TaskResult> done = ForeignCallbackIT.done("error");
        Assertions.assertEquals(
            Optional.of(List.of(done.get(0).id())), done.get(1).value()
        );
    }

    @Test
    void marksTheTaskThatFailedAsFailed() {
        Assertions.assertEquals(
            new State(State.FAILURE), ForeignCallbackIT.done("error").get(0).state()
        );
    }

    @Test
    void keepsTheForeignSideRunning() {
        Assertions.assertTrue(ForeignCallbackIT.FOREIGN.isRunning());
    }

    private static List<TaskResult> done(final String shape) {
        final String queue = String.format("attached-%s", UUID.randomUUID());
        ForeignCallbackIT.ask(shape, queue);
        try (Broker broker = new RedisBroker(ForeignCallbackIT.pool())) {
            return new Worker(
                broker,
                new FakeBackend(new ConcurrentHashMap<>()),
                new Registry(
                    Map.of(
                        "proj.java.echo",
                        task -> task.args(),
                        "proj.java.boom",
                        task -> {
                            throw new IllegalStateException("as asked for");
                        }
                    )
                )
            ).some(queue, ForeignCallbackIT.WAIT, 2);
        }
    }

    private static void ask(final String shape, final String queue) {
        try {
            ForeignCallbackIT.FOREIGN.execInContainer("python", "workflow.py", shape, queue);
        } catch (final IOException ex) {
            throw new IllegalStateException("the foreign producer could not be run", ex);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("running the foreign producer was interrupted", ex);
        }
    }

    private static GenericContainer<?> foreign() {
        return new GenericContainer<>(ForeignCallbackIT.image())
            .withNetwork(ForeignCallbackIT.NETWORK)
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
            ForeignCallbackIT.STORE.getHost(),
            ForeignCallbackIT.STORE.getMappedPort(ForeignCallbackIT.PORT)
        );
    }
}
