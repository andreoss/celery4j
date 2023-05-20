/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.stack;

import io.celery4j.protocol.JsonMessageCodec;
import io.celery4j.protocol.ProtocolV2;
import io.celery4j.protocol.Task;
import io.celery4j.result.Backend;
import io.celery4j.result.RedisBackend;
import io.celery4j.result.TaskResult;
import io.celery4j.transport.Broker;
import io.celery4j.transport.Queues;
import io.celery4j.transport.RedisBroker;
import io.celery4j.worker.Registry;
import io.celery4j.worker.Worker;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import redis.clients.jedis.JedisPool;

/**
 * Live test case for two consumers competing for one queue.
 *
 * <p>The fleet cases run several workers inside one process against one broker
 * object. Two brokers, each with its own connection and its own bookkeeping,
 * are what a deployment has, and are the only way to find out whether a
 * message can reach both of them.</p>
 *
 * @since 0.3.1
 */
@Tag("live")
@Testcontainers
final class CompetitionIT {

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
    private static final int MANY = 40;

    /**
     * How long a receive waits.
     */
    private static final Duration WAIT = Duration.ofMillis(300L);

    /**
     * The store the cases run against.
     */
    @Container
    private static final GenericContainer<?> STORE =
        new GenericContainer<>("docker.io/library/redis:7-alpine")
            .withExposedPorts(CompetitionIT.PORT);

    @Test
    void runsEveryTaskBetweenTheTwo() {
        Assertions.assertEquals(
            CompetitionIT.MANY, CompetitionIT.shared(new ConcurrentLinkedQueue<>())
        );
    }

    @Test
    void runsNoTaskTwiceBetweenTheTwo() {
        final Queue<Object> seen = new ConcurrentLinkedQueue<>();
        CompetitionIT.shared(seen);
        Assertions.assertEquals(CompetitionIT.MANY, Set.copyOf(seen).size());
    }

    @Test
    void leavesAResultForEveryTask() {
        final Queue<Object> seen = new ConcurrentLinkedQueue<>();
        CompetitionIT.shared(seen, CompetitionIT.queue());
        try (Backend backend = new RedisBackend(CompetitionIT.pool())) {
            Assertions.assertEquals(
                CompetitionIT.MANY,
                seen.stream()
                    .map(String::valueOf)
                    .filter(id -> backend.of(id).map(TaskResult::state).isPresent())
                    .count()
            );
        }
    }

    @Test
    void sharesTheWorkBetweenBoth() {
        Assertions.assertTrue(CompetitionIT.split(new ConcurrentLinkedQueue<>()) > 0L);
    }

    private static long shared(final Queue<Object> seen) {
        return CompetitionIT.shared(seen, CompetitionIT.queue());
    }

    private static long shared(final Queue<Object> seen, final String queue) {
        return CompetitionIT.taken(seen, queue).stream().mapToLong(Long::longValue).sum();
    }

    private static long split(final Queue<Object> seen) {
        return CompetitionIT.taken(seen, CompetitionIT.queue())
            .stream()
            .filter(taken -> taken > 0L)
            .count();
    }

    private static List<Long> taken(final Queue<Object> seen, final String queue) {
        CompetitionIT.fill(queue);
        final List<Long> counts = new CopyOnWriteArrayList<>();
        final Thread one = new Thread(() -> counts.add(CompetitionIT.worked(seen, queue)));
        final Thread two = new Thread(() -> counts.add(CompetitionIT.worked(seen, queue)));
        one.start();
        two.start();
        CompetitionIT.joined(one);
        CompetitionIT.joined(two);
        return List.copyOf(counts);
    }

    private static void joined(final Thread worker) {
        try {
            worker.join();
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("waiting for a worker was interrupted", ex);
        }
    }

    private static long worked(final Queue<Object> seen, final String queue) {
        try (
            Broker broker = new RedisBroker(
                CompetitionIT.pool(),
                new JsonMessageCodec(),
                new Queues(),
                String.format("held-%s", UUID.randomUUID())
            );
            Backend backend = new RedisBackend(CompetitionIT.pool())
        ) {
            return new Worker(
                broker,
                backend,
                new Registry(
                    Map.of(
                        CompetitionIT.NAME,
                        task -> {
                            seen.add(task.id());
                            return task.id();
                        }
                    )
                )
            ).some(queue, CompetitionIT.WAIT, CompetitionIT.MANY).size();
        }
    }

    private static void fill(final String queue) {
        try (Broker broker = new RedisBroker(CompetitionIT.pool())) {
            final ProtocolV2 protocol = new ProtocolV2();
            for (int index = 0; index < CompetitionIT.MANY; index = index + 1) {
                broker.send(
                    protocol.message(
                        new Task(
                            UUID.randomUUID().toString(),
                            CompetitionIT.NAME,
                            List.of(index),
                            Map.of()
                        ),
                        queue
                    ),
                    queue
                );
            }
        }
    }

    private static String queue() {
        return String.format("competition-%s", UUID.randomUUID());
    }

    private static JedisPool pool() {
        return new JedisPool(
            CompetitionIT.STORE.getHost(), CompetitionIT.STORE.getMappedPort(CompetitionIT.PORT)
        );
    }
}
