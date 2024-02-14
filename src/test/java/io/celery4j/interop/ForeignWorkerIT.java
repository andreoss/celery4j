/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2024
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.interop;

import io.celery4j.protocol.ProtocolV2;
import io.celery4j.protocol.Task;
import io.celery4j.result.Backend;
import io.celery4j.result.RedisBackend;
import io.celery4j.result.Results;
import io.celery4j.result.State;
import io.celery4j.result.TaskResult;
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
 * Live test case putting a worker of the original implementation on the other
 * end of the queue.
 *
 * <p>This is the evidence R-14 asks for: the tests here are not written
 * against this library's own reading of the protocol, but against software
 * that defines it.</p>
 *
 * @since 0.1.1
 */
@Tag("live")
@Testcontainers
final class ForeignWorkerIT {

    /**
     * Port the store listens on inside its container.
     */
    private static final int PORT = 6379;

    /**
     * Queue the foreign worker reads.
     */
    private static final String QUEUE = "interop";

    /**
     * Name of the task the foreign worker answers for.
     */
    private static final String ADD = "proj.tasks.add";

    /**
     * How long a foreign worker may take to start and finish a task. The
     * first case pays for the worker booting, which is the readiness this
     * suite waits on: a worker is ready when it answers, not when it says so.
     */
    private static final Duration PATIENCE = Duration.ofSeconds(90L);

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
            .withExposedPorts(ForeignWorkerIT.PORT)
            .withNetwork(ForeignWorkerIT.NETWORK)
            .withNetworkAliases("store");

    /**
     * A worker of the original implementation.
     */
    @Container
    private static final GenericContainer<?> FOREIGN = ForeignWorkerIT.foreign();

    @Test
    void hasItsTaskRunByAForeignWorker() {
        final String id = UUID.randomUUID().toString();
        ForeignWorkerIT.send(id, ForeignWorkerIT.ADD, List.of(2, 2));
        Assertions.assertEquals(
            Optional.of(4), ForeignWorkerIT.finished(id).value()
        );
    }

    @Test
    void readsTheStateAForeignWorkerWrote() {
        final String id = UUID.randomUUID().toString();
        ForeignWorkerIT.send(id, ForeignWorkerIT.ADD, List.of(3, 4));
        Assertions.assertEquals(
            new State(State.SUCCESS), ForeignWorkerIT.finished(id).state()
        );
    }

    @Test
    void readsTheIdentifierAForeignWorkerWrote() {
        final String id = UUID.randomUUID().toString();
        ForeignWorkerIT.send(id, ForeignWorkerIT.ADD, List.of(1, 1));
        Assertions.assertEquals(id, ForeignWorkerIT.finished(id).id());
    }

    @Test
    void readsTheFailureAForeignWorkerWrote() {
        final String id = UUID.randomUUID().toString();
        ForeignWorkerIT.send(id, "proj.tasks.boom", List.of());
        Assertions.assertEquals(
            new State(State.FAILURE), ForeignWorkerIT.finished(id).state()
        );
    }

    @Test
    void readsWhatTheForeignFailureSaid() {
        final String id = UUID.randomUUID().toString();
        ForeignWorkerIT.send(id, "proj.tasks.boom", List.of());
        Assertions.assertTrue(
            ForeignWorkerIT.finished(id).traceback().orElseThrow().contains("ValueError")
        );
    }

    @Test
    void keepsAForeignWorkerRunning() {
        Assertions.assertTrue(ForeignWorkerIT.FOREIGN.isRunning());
    }

    private static void send(final String id, final String name, final List<Object> args) {
        try (Broker broker = new RedisBroker(ForeignWorkerIT.pool())) {
            broker.send(
                new ProtocolV2().message(
                    new Task(id, name, args, Map.of()), ForeignWorkerIT.QUEUE
                ),
                ForeignWorkerIT.QUEUE
            );
        }
    }

    private static TaskResult finished(final String id) {
        try (Backend backend = new RedisBackend(ForeignWorkerIT.pool())) {
            return new Results(backend, Duration.ofMillis(200L))
                .await(id, ForeignWorkerIT.PATIENCE)
                .orElseThrow();
        }
    }

    private static GenericContainer<?> foreign() {
        return new GenericContainer<>(ForeignWorkerIT.image())
            .withNetwork(ForeignWorkerIT.NETWORK)
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
            ForeignWorkerIT.STORE.getHost(),
            ForeignWorkerIT.STORE.getMappedPort(ForeignWorkerIT.PORT)
        );
    }
}
