/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.interop;

import io.celery4j.protocol.ProtocolV2;
import io.celery4j.protocol.Task;
import io.celery4j.result.Backend;
import io.celery4j.result.RedisBackend;
import io.celery4j.result.Results;
import io.celery4j.result.State;
import io.celery4j.transport.Broker;
import io.celery4j.transport.RedisBroker;
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
import redis.clients.jedis.JedisPool;

/**
 * Live test case for the states that are not the end of anything.
 *
 * <p>A worker of the original implementation writes that a task has started,
 * that it is waiting to run again, and that it was called off. This library
 * claims to read all three, and until now had written all three itself.</p>
 *
 * @since 0.3.0
 */
@Tag("live")
@Testcontainers
final class ForeignStatesIT {

    /**
     * Port the store listens on inside its container.
     */
    private static final int PORT = 6379;

    /**
     * Queue the foreign worker reads.
     */
    private static final String QUEUE = "interop";

    /**
     * How long a state written by the other side may take to appear.
     */
    private static final Duration PATIENCE = Duration.ofSeconds(60L);

    /**
     * Network the store and the worker meet on.
     */
    private static final Network NETWORK = Network.newNetwork();

    /**
     * The store both sides use.
     */
    @Container
    private static final GenericContainer<?> STORE =
        new GenericContainer<>("docker.io/library/redis:7-alpine")
            .withExposedPorts(ForeignStatesIT.PORT)
            .withNetwork(ForeignStatesIT.NETWORK)
            .withNetworkAliases("store");

    /**
     * A worker of the original implementation.
     */
    @Container
    private static final GenericContainer<?> FOREIGN = ForeignStatesIT.foreign();

    @Test
    void readsThatATaskHasStarted() {
        Assertions.assertEquals(
            State.STARTED,
            ForeignStatesIT.reached("proj.tasks.slow", List.of(5), State.STARTED).name()
        );
    }

    @Test
    void treatsAStartedTaskAsUnfinished() {
        Assertions.assertFalse(
            ForeignStatesIT.reached("proj.tasks.slow", List.of(5), State.STARTED).terminal()
        );
    }

    @Test
    void waitsThroughAStartedTask() {
        final String id = UUID.randomUUID().toString();
        ForeignStatesIT.send(id, "proj.tasks.slow", List.of(5));
        try (Backend backend = new RedisBackend(ForeignStatesIT.pool())) {
            Assertions.assertEquals(
                Optional.empty(),
                new Results(backend, Duration.ofMillis(200L))
                    .await(id, Duration.ofSeconds(2L))
            );
        }
    }

    @Test
    void readsThatATaskIsWaitingToRunAgain() {
        Assertions.assertEquals(
            State.RETRY,
            ForeignStatesIT.reached("proj.tasks.flaky", List.of(), State.RETRY).name()
        );
    }

    @Test
    void treatsATaskWaitingToRunAgainAsUnfinished() {
        Assertions.assertFalse(
            ForeignStatesIT.reached("proj.tasks.flaky", List.of(), State.RETRY).terminal()
        );
    }

    @Test
    void keepsAForeignWorkerRunning() {
        Assertions.assertTrue(ForeignStatesIT.FOREIGN.isRunning());
    }

    private static State reached(
        final String name, final List<Object> args, final String wanted
    ) {
        final String id = UUID.randomUUID().toString();
        ForeignStatesIT.send(id, name, args);
        try (Backend backend = new RedisBackend(ForeignStatesIT.pool())) {
            final long deadline = System.nanoTime() + ForeignStatesIT.PATIENCE.toNanos();
            State state = new State(State.PENDING);
            while (System.nanoTime() < deadline) {
                state = backend.of(id).map(result -> result.state()).orElse(state);
                if (wanted.equals(state.name())) {
                    break;
                }
                ForeignStatesIT.pause();
            }
            return state;
        }
    }

    private static void send(final String id, final String name, final List<Object> args) {
        try (Broker broker = new RedisBroker(ForeignStatesIT.pool())) {
            broker.send(
                new ProtocolV2().message(
                    new Task(id, name, args, Map.of()), ForeignStatesIT.QUEUE
                ),
                ForeignStatesIT.QUEUE
            );
        }
    }

    private static void pause() {
        try {
            Thread.sleep(100L);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("waiting for a state was interrupted", ex);
        }
    }

    private static GenericContainer<?> foreign() {
        return new GenericContainer<>(ForeignStatesIT.image())
            .withNetwork(ForeignStatesIT.NETWORK)
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
            ForeignStatesIT.STORE.getHost(),
            ForeignStatesIT.STORE.getMappedPort(ForeignStatesIT.PORT)
        );
    }
}
