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
import io.celery4j.result.TaskResult;
import io.celery4j.transport.Broker;
import io.celery4j.transport.RedisBroker;
import io.celery4j.worker.Registry;
import io.celery4j.worker.Worker;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import redis.clients.jedis.JedisPool;

/**
 * Live test case for a queue that a foreign worker and a worker of this
 * library both read.
 *
 * <p>Which of two consumers gets a given message is nobody's promise, so no
 * case here waits to see how a race turns out. Instead the foreign worker is
 * paused while this library's worker takes its share and resumed while it
 * takes the rest, and what is checked is that every task was run and that each
 * was run by exactly one side.</p>
 *
 * @since 1.0.0
 */
@Tag("live")
@Testcontainers
final class MixedConsumersIT {

    /**
     * Port the store listens on inside its container.
     */
    private static final int PORT = 6379;

    /**
     * Queue both sides read.
     */
    private static final String QUEUE = "interop";

    /**
     * Name of the task both sides answer for.
     */
    private static final String ADD = "proj.tasks.add";

    /**
     * What this library's worker returns, so a result says who ran it.
     */
    private static final String MINE = "ran-here";

    /**
     * How many tasks each side is given.
     */
    private static final int EACH = 3;

    /**
     * How long a receive waits.
     */
    private static final Duration WAIT = Duration.ofMillis(300L);

    /**
     * How long a task may take to be run by either side.
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
            .withExposedPorts(MixedConsumersIT.PORT)
            .withNetwork(MixedConsumersIT.NETWORK)
            .withNetworkAliases("store");

    /**
     * A worker of the original implementation.
     */
    @Container
    private static final GenericContainer<?> FOREIGN = MixedConsumersIT.foreign();

    @Test
    void runsWhatItTookFromTheSharedQueue() {
        Assertions.assertEquals(
            MixedConsumersIT.EACH,
            MixedConsumersIT.mine().stream().filter(MixedConsumersIT.MINE::equals).count()
        );
    }

    @Test
    void leavesTheRestToTheOtherSide() {
        Assertions.assertEquals(
            MixedConsumersIT.EACH,
            MixedConsumersIT.theirs().stream()
                .filter(value -> !MixedConsumersIT.MINE.equals(value))
                .count()
        );
    }

    @Test
    void runsNothingOfWhatTheOtherSideTook() {
        Assertions.assertEquals(
            0L,
            MixedConsumersIT.theirs().stream().filter(MixedConsumersIT.MINE::equals).count()
        );
    }

    @Test
    void keepsAForeignWorkerRunning() {
        Assertions.assertTrue(MixedConsumersIT.FOREIGN.isRunning());
    }

    private static List<Object> mine() {
        MixedConsumersIT.pause();
        try {
            final List<String> ids = MixedConsumersIT.sent();
            MixedConsumersIT.work();
            return MixedConsumersIT.values(ids);
        } finally {
            MixedConsumersIT.resume();
        }
    }

    private static List<Object> theirs() {
        MixedConsumersIT.resume();
        return MixedConsumersIT.values(MixedConsumersIT.sent());
    }

    private static List<Object> values(final List<String> ids) {
        final List<Object> found = new ArrayList<>(ids.size());
        try (Backend backend = new RedisBackend(MixedConsumersIT.pool())) {
            final Results results = new Results(backend, Duration.ofMillis(200L));
            for (final String id : ids) {
                results.await(id, MixedConsumersIT.PATIENCE)
                    .flatMap(TaskResult::value)
                    .ifPresent(found::add);
            }
        }
        return List.copyOf(found);
    }

    private static List<String> sent() {
        final List<String> ids = new ArrayList<>(MixedConsumersIT.EACH);
        try (Broker broker = new RedisBroker(MixedConsumersIT.pool())) {
            final ProtocolV2 protocol = new ProtocolV2();
            for (int index = 0; index < MixedConsumersIT.EACH; index = index + 1) {
                final String id = UUID.randomUUID().toString();
                ids.add(id);
                broker.send(
                    protocol.message(
                        new Task(id, MixedConsumersIT.ADD, List.of(2, 2), Map.of()),
                        MixedConsumersIT.QUEUE
                    ),
                    MixedConsumersIT.QUEUE
                );
            }
        }
        return List.copyOf(ids);
    }

    private static void work() {
        try (
            Broker broker = new RedisBroker(MixedConsumersIT.pool());
            Backend backend = new RedisBackend(MixedConsumersIT.pool())
        ) {
            new Worker(
                broker,
                backend,
                new Registry(Map.of(MixedConsumersIT.ADD, task -> MixedConsumersIT.MINE))
            ).some(MixedConsumersIT.QUEUE, MixedConsumersIT.WAIT, MixedConsumersIT.EACH);
        }
    }

    private static void pause() {
        if (MixedConsumersIT.paused()) {
            return;
        }
        DockerClientFactory.instance()
            .client()
            .pauseContainerCmd(MixedConsumersIT.FOREIGN.getContainerId())
            .exec();
    }

    private static void resume() {
        if (MixedConsumersIT.paused()) {
            DockerClientFactory.instance()
                .client()
                .unpauseContainerCmd(MixedConsumersIT.FOREIGN.getContainerId())
                .exec();
        }
    }

    private static boolean paused() {
        return Boolean.TRUE.equals(
            DockerClientFactory.instance()
                .client()
                .inspectContainerCmd(MixedConsumersIT.FOREIGN.getContainerId())
                .exec()
                .getState()
                .getPaused()
        );
    }

    private static GenericContainer<?> foreign() {
        return new GenericContainer<>(MixedConsumersIT.image())
            .withNetwork(MixedConsumersIT.NETWORK)
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
            MixedConsumersIT.STORE.getHost(),
            MixedConsumersIT.STORE.getMappedPort(MixedConsumersIT.PORT)
        );
    }
}
