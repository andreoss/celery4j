/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.interop;

import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageProperties;
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
 * Live test case for the queue names a priority is carried on.
 *
 * <p>The naming was read out of the protocol's transport and has been checked
 * only against this library's own consumer. Here a worker of the original
 * implementation is the consumer, so the name has to be the one it listens
 * on.</p>
 *
 * @since 0.2.1
 */
@Tag("live")
@Testcontainers
final class ForeignPriorityIT {

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
            .withExposedPorts(ForeignPriorityIT.PORT)
            .withNetwork(ForeignPriorityIT.NETWORK)
            .withNetworkAliases("store");

    /**
     * A worker of the original implementation.
     */
    @Container
    private static final GenericContainer<?> FOREIGN = ForeignPriorityIT.foreign();

    @Test
    void hasATaskOfTheLowestPriorityRun() {
        Assertions.assertEquals(Optional.of(4), ForeignPriorityIT.answered(0).value());
    }

    @Test
    void hasATaskOfAMiddlePriorityRun() {
        Assertions.assertEquals(Optional.of(4), ForeignPriorityIT.answered(3).value());
    }

    @Test
    void hasATaskOfAHigherPriorityRun() {
        Assertions.assertEquals(Optional.of(4), ForeignPriorityIT.answered(6).value());
    }

    @Test
    void hasATaskOfTheHighestPriorityRun() {
        Assertions.assertEquals(Optional.of(4), ForeignPriorityIT.answered(9).value());
    }

    @Test
    void readsTheStateOfAPrioritisedTask() {
        Assertions.assertEquals(
            new State(State.SUCCESS), ForeignPriorityIT.answered(6).state()
        );
    }

    @Test
    void keepsAForeignWorkerRunning() {
        Assertions.assertTrue(ForeignPriorityIT.FOREIGN.isRunning());
    }

    private static TaskResult answered(final int priority) {
        final String id = UUID.randomUUID().toString();
        try (
            Broker broker = new RedisBroker(ForeignPriorityIT.pool());
            Backend backend = new RedisBackend(ForeignPriorityIT.pool())
        ) {
            final Message plain = new ProtocolV2().message(
                new Task(id, "proj.tasks.add", List.of(2, 2), Map.of()),
                ForeignPriorityIT.QUEUE
            );
            broker.send(
                new Message(
                    plain.properties().with(MessageProperties.PRIORITY, priority),
                    plain.headers(),
                    plain.body()
                ),
                ForeignPriorityIT.QUEUE
            );
            return new Results(backend, Duration.ofMillis(200L))
                .await(id, ForeignPriorityIT.PATIENCE)
                .orElseThrow();
        }
    }

    private static GenericContainer<?> foreign() {
        return new GenericContainer<>(ForeignPriorityIT.image())
            .withNetwork(ForeignPriorityIT.NETWORK)
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
            .withFileFromClasspath("read.py", "live/read.py");
    }

    private static JedisPool pool() {
        return new JedisPool(
            ForeignPriorityIT.STORE.getHost(),
            ForeignPriorityIT.STORE.getMappedPort(ForeignPriorityIT.PORT)
        );
    }
}
