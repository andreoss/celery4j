/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.stack;

import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageHeaders;
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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Live test case for a task that outruns its time limit, with real services in
 * the path.
 *
 * <p>The limit was checked against fakes, where nothing is really slow. Here
 * the message comes off a real queue and the failure goes into a real store,
 * which is where anyone waiting will look for it.</p>
 *
 * <p>The store is given a generous timeout, because a container runtime under
 * load answers more slowly than any service does.</p>
 *
 * @since 0.3.0
 */
@Tag("live")
@Testcontainers
final class LimitIT {

    /**
     * Port the store listens on inside its container.
     */
    private static final int PORT = 6379;

    /**
     * Name of the task these cases run.
     */
    private static final String NAME = "proj.java.slow";

    /**
     * How long a receive waits.
     */
    private static final Duration WAIT = Duration.ofSeconds(2L);

    /**
     * The store the cases run against.
     */
    @Container
    private static final GenericContainer<?> STORE =
        new GenericContainer<>("docker.io/library/redis:7-alpine")
            .withExposedPorts(LimitIT.PORT);

    @Test
    void failsATaskThatOutranItsLimit() {
        Assertions.assertEquals(
            new State(State.FAILURE), LimitIT.ran(LimitIT.slow()).state()
        );
    }

    @Test
    void saysWhyATaskThatOutranItsLimitFailed() {
        Assertions.assertEquals(
            "TimeoutException",
            ((Map<?, ?>) LimitIT.ran(LimitIT.slow()).value().orElseThrow()).get(Worker.TYPE)
        );
    }

    @Test
    void leavesThatFailureInTheStore() {
        final String id = UUID.randomUUID().toString();
        LimitIT.ran(LimitIT.slow(), id);
        try (Backend backend = new RedisBackend(LimitIT.pool())) {
            Assertions.assertEquals(
                Optional.of(new State(State.FAILURE)),
                backend.of(id).map(TaskResult::state)
            );
        }
    }

    @Test
    void interruptsTheTaskThatOutranIt() {
        final AtomicBoolean interrupted = new AtomicBoolean();
        LimitIT.ran(LimitIT.watching(interrupted));
        Assertions.assertTrue(interrupted.get());
    }

    @Test
    void runsATaskThatKeepsWithinItsLimit() {
        Assertions.assertEquals(
            new State(State.SUCCESS), LimitIT.ran(task -> 4).state()
        );
    }

    private static Job slow() {
        return task -> {
            try {
                Thread.sleep(3_000L);
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            return 4;
        };
    }

    private static Job watching(final AtomicBoolean interrupted) {
        return task -> {
            try {
                Thread.sleep(3_000L);
            } catch (final InterruptedException ex) {
                interrupted.set(true);
                Thread.currentThread().interrupt();
            }
            return 4;
        };
    }

    private static TaskResult ran(final Job job) {
        return LimitIT.ran(job, UUID.randomUUID().toString());
    }

    private static TaskResult ran(final Job job, final String id) {
        final String queue = String.format("limit-%s", UUID.randomUUID());
        try (
            Broker broker = new RedisBroker(LimitIT.pool());
            Backend backend = new RedisBackend(LimitIT.pool())
        ) {
            broker.send(LimitIT.limited(id, queue), queue);
            return new Worker(broker, backend, new Registry(Map.of(LimitIT.NAME, job)))
                .once(queue, LimitIT.WAIT)
                .orElseThrow();
        }
    }

    private static Message limited(final String id, final String queue) {
        final Message message = new ProtocolV2().message(
            new Task(id, LimitIT.NAME, List.of(2, 2), Map.of()), queue
        );
        return new Message(
            message.properties(),
            message.headers().with(MessageHeaders.TIMELIMIT, List.of(1, 2)),
            message.body()
        );
    }

    private static JedisPool pool() {
        return new JedisPool(
            new JedisPoolConfig(),
            LimitIT.STORE.getHost(),
            LimitIT.STORE.getMappedPort(LimitIT.PORT),
            15_000
        );
    }
}
