/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2024
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.interop;

import io.celery4j.protocol.ProtocolV1;
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
 * Live test case for the older protocol against the framework that defines it.
 *
 * <p>This library writes version one and, until now, had only its own reading
 * of that version as evidence. R-14 calls that no evidence at all, and this is
 * what it asks for instead.</p>
 *
 * @since 0.2.1
 */
@Tag("live")
@Testcontainers
final class ForeignOlderProtocolIT {

    /**
     * Port the store listens on inside its container.
     */
    private static final int PORT = 6379;

    /**
     * Queue the foreign worker reads.
     */
    private static final String QUEUE = "interop";

    /**
     * How long a foreign worker may take to start and finish a task.
     */
    private static final Duration PATIENCE = Duration.ofMinutes(2L);

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
            .withExposedPorts(ForeignOlderProtocolIT.PORT)
            .withNetwork(ForeignOlderProtocolIT.NETWORK)
            .withNetworkAliases("store");

    /**
     * A worker of the original implementation.
     */
    @Container
    private static final GenericContainer<?> FOREIGN = ForeignOlderProtocolIT.foreign();

    @Test
    void hasAnOlderMessageRunByAForeignWorker() {
        Assertions.assertEquals(
            Optional.of(4), ForeignOlderProtocolIT.answered(List.of(2, 2)).value()
        );
    }

    @Test
    void readsTheStateOfAnOlderMessage() {
        Assertions.assertEquals(
            new State(State.SUCCESS), ForeignOlderProtocolIT.answered(List.of(1, 1)).state()
        );
    }

    @Test
    void carriesTheParametersOfAnOlderMessage() {
        Assertions.assertEquals(
            Optional.of(9), ForeignOlderProtocolIT.answered(List.of(4, 5)).value()
        );
    }

    @Test
    void keepsAForeignWorkerRunning() {
        Assertions.assertTrue(ForeignOlderProtocolIT.FOREIGN.isRunning());
    }

    private static TaskResult answered(final List<Object> args) {
        final String id = UUID.randomUUID().toString();
        try (
            Broker broker = new RedisBroker(ForeignOlderProtocolIT.pool());
            Backend backend = new RedisBackend(ForeignOlderProtocolIT.pool())
        ) {
            broker.send(
                new ProtocolV1().message(
                    new Task(id, "proj.tasks.add", args, Map.of()),
                    ForeignOlderProtocolIT.QUEUE
                ),
                ForeignOlderProtocolIT.QUEUE
            );
            return new Results(backend, Duration.ofMillis(200L))
                .await(id, ForeignOlderProtocolIT.PATIENCE)
                .orElseThrow();
        }
    }

    private static GenericContainer<?> foreign() {
        return new GenericContainer<>(ForeignOlderProtocolIT.image())
            .withNetwork(ForeignOlderProtocolIT.NETWORK)
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
            ForeignOlderProtocolIT.STORE.getHost(),
            ForeignOlderProtocolIT.STORE.getMappedPort(ForeignOlderProtocolIT.PORT)
        );
    }
}
