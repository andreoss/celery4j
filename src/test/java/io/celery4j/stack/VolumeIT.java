/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.stack;

import io.celery4j.protocol.ProtocolV2;
import io.celery4j.protocol.Task;
import io.celery4j.result.Backend;
import io.celery4j.result.RedisBackend;
import io.celery4j.result.State;
import io.celery4j.result.TaskResult;
import io.celery4j.transport.Broker;
import io.celery4j.transport.RedisBroker;
import io.celery4j.worker.Job;
import io.celery4j.worker.Registry;
import io.celery4j.worker.Worker;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import redis.clients.jedis.JedisPool;

/**
 * Live test case for what happens when a hundred tasks arrive at once.
 *
 * <p>A case that sends one message cannot tell a broker that loses one from a
 * broker that runs one twice, and neither from one that reorders. These send
 * many and count.</p>
 *
 * @since 0.2.1
 */
@Tag("live")
@Testcontainers
final class VolumeIT {

    /**
     * Port the store listens on inside its container.
     */
    private static final int PORT = 6379;

    /**
     * Name of the task these cases run.
     */
    private static final String NAME = "proj.java.count";

    /**
     * How many tasks a case sends.
     */
    private static final int MANY = 100;

    /**
     * How long a receive waits.
     */
    private static final Duration WAIT = Duration.ofMillis(200L);

    /**
     * The store the cases run against.
     */
    @Container
    private static final GenericContainer<?> STORE =
        new GenericContainer<>("docker.io/library/redis:7-alpine")
            .withExposedPorts(VolumeIT.PORT);

    @Test
    void runsEveryTaskThatWasSent() {
        Assertions.assertEquals(
            VolumeIT.MANY,
            VolumeIT.through(new ConcurrentLinkedQueue<>(), VolumeIT.queue("every")).size()
        );
    }

    @Test
    void runsNoTaskTwice() {
        final Queue<Object> seen = new ConcurrentLinkedQueue<>();
        VolumeIT.through(seen, VolumeIT.queue("twice"));
        Assertions.assertEquals(VolumeIT.MANY, Set.copyOf(seen).size());
    }

    @Test
    void takesThemInTheOrderTheyWereSent() {
        final Queue<Object> seen = new ConcurrentLinkedQueue<>();
        VolumeIT.through(seen, VolumeIT.queue("order"));
        Assertions.assertEquals(
            IntStream.range(0, VolumeIT.MANY).boxed().map(Object.class::cast).toList(),
            List.copyOf(seen)
        );
    }

    @Test
    void publishesAResultForEveryTask() {
        Assertions.assertEquals(
            (long) VolumeIT.MANY,
            VolumeIT.through(new ConcurrentLinkedQueue<>(), VolumeIT.queue("results"))
                .stream()
                .map(TaskResult::state)
                .filter(State::successful)
                .count()
        );
    }

    @Test
    void leavesTheQueueEmpty() {
        final String queue = VolumeIT.queue("empty");
        VolumeIT.through(new ConcurrentLinkedQueue<>(), queue);
        try (Broker broker = new RedisBroker(VolumeIT.pool())) {
            Assertions.assertTrue(broker.receive(queue, VolumeIT.WAIT).isEmpty());
        }
    }

    private static List<TaskResult> through(
        final Queue<Object> seen, final String queue
    ) {
        try (
            Broker broker = new RedisBroker(VolumeIT.pool());
            Backend backend = new RedisBackend(VolumeIT.pool())
        ) {
            final ProtocolV2 protocol = new ProtocolV2();
            for (int index = 0; index < VolumeIT.MANY; index = index + 1) {
                broker.send(
                    protocol.message(
                        new Task(
                            UUID.randomUUID().toString(),
                            VolumeIT.NAME,
                            List.of(index),
                            Map.of()
                        ),
                        queue
                    ),
                    queue
                );
            }
            return new Worker(
                broker,
                backend,
                new Registry(Map.of(VolumeIT.NAME, VolumeIT.counting(seen)))
            ).some(queue, VolumeIT.WAIT, VolumeIT.MANY);
        }
    }

    private static Job counting(final Queue<Object> seen) {
        return task -> {
            seen.add(task.args().get(0));
            return task.args().get(0);
        };
    }

    private static String queue(final String suffix) {
        return String.format("volume-%s-%s", suffix, UUID.randomUUID());
    }

    private static JedisPool pool() {
        return new JedisPool(
            VolumeIT.STORE.getHost(), VolumeIT.STORE.getMappedPort(VolumeIT.PORT)
        );
    }
}
