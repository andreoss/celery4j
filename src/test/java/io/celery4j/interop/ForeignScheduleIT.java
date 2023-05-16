/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
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
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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
 * Live test case for what the other side says about time.
 *
 * <p>A delay and an expiry are written by the framework that defines them, not
 * by this library, so what is checked here is agreement rather than
 * consistency.</p>
 *
 * @since 0.2.1
 */
@Tag("live")
@Testcontainers
final class ForeignScheduleIT {

    /**
     * Port the store listens on inside its container.
     */
    private static final int PORT = 6379;

    /**
     * How long a case waits for something that should not happen.
     */
    private static final Duration GLANCE = Duration.ofMillis(700L);

    /**
     * Name of the task the foreign producer asks for.
     */
    private static final String ECHO = "proj.java.echo";

    /**
     * Network the store and the producer meet on.
     */
    private static final Network NETWORK = Network.newNetwork();

    /**
     * The store both sides use.
     */
    @Container
    private static final GenericContainer<?> STORE =
        new GenericContainer<>("docker.io/library/redis:7-alpine")
            .withExposedPorts(ForeignScheduleIT.PORT)
            .withNetwork(ForeignScheduleIT.NETWORK)
            .withNetworkAliases("store");

    /**
     * The foreign side, holding the producer this suite runs.
     */
    @Container
    private static final GenericContainer<?> FOREIGN = ForeignScheduleIT.foreign();

    @Test
    void waitsForADelayTheOtherSideSet() {
        final String queue = ForeignScheduleIT.queue();
        ForeignScheduleIT.ask(queue, "3", "-");
        Assertions.assertEquals(
            Optional.empty(), ForeignScheduleIT.handled(new AtomicInteger(), queue)
        );
    }

    @Test
    void leavesADelayedTaskUnrunForNow() {
        final AtomicInteger runs = new AtomicInteger();
        final String queue = ForeignScheduleIT.queue();
        ForeignScheduleIT.ask(queue, "3", "-");
        ForeignScheduleIT.handled(runs, queue);
        Assertions.assertEquals(0, runs.get());
    }

    @Test
    void runsATaskOnceItsDelayHasPassed() {
        final AtomicInteger runs = new AtomicInteger();
        final String queue = ForeignScheduleIT.queue();
        ForeignScheduleIT.ask(queue, "1", "-");
        ForeignScheduleIT.handled(runs, queue);
        ForeignScheduleIT.pause(Duration.ofSeconds(2L));
        Assertions.assertEquals(
            Optional.of(new State(State.SUCCESS)),
            ForeignScheduleIT.handled(runs, queue).map(TaskResult::state)
        );
    }

    @Test
    void callsOffATaskTheOtherSideLetExpire() {
        final String queue = ForeignScheduleIT.queue();
        ForeignScheduleIT.ask(queue, "-", "0");
        ForeignScheduleIT.pause(Duration.ofMillis(500L));
        Assertions.assertEquals(
            Optional.of(new State(State.REVOKED)),
            ForeignScheduleIT.handled(new AtomicInteger(), queue).map(TaskResult::state)
        );
    }

    @Test
    void doesNotRunATaskTheOtherSideLetExpire() {
        final AtomicInteger runs = new AtomicInteger();
        final String queue = ForeignScheduleIT.queue();
        ForeignScheduleIT.ask(queue, "-", "0");
        ForeignScheduleIT.pause(Duration.ofMillis(500L));
        ForeignScheduleIT.handled(runs, queue);
        Assertions.assertEquals(0, runs.get());
    }

    @Test
    void keepsTheForeignSideRunning() {
        Assertions.assertTrue(ForeignScheduleIT.FOREIGN.isRunning());
    }

    private static Optional<TaskResult> handled(final AtomicInteger runs, final String queue) {
        try (Broker broker = new RedisBroker(ForeignScheduleIT.pool())) {
            return new Worker(
                broker,
                new FakeBackend(new ConcurrentHashMap<>()),
                new Registry(
                    Map.of(
                        ForeignScheduleIT.ECHO,
                        task -> runs.incrementAndGet()
                    )
                )
            ).once(queue, ForeignScheduleIT.GLANCE);
        }
    }

    private static void ask(
        final String queue, final String countdown, final String expires
    ) {
        try {
            ForeignScheduleIT.FOREIGN.execInContainer(
                "python",
                "schedule.py",
                UUID.randomUUID().toString(),
                ForeignScheduleIT.ECHO,
                queue,
                countdown,
                expires
            );
        } catch (final IOException ex) {
            throw new IllegalStateException("the foreign producer could not be run", ex);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("running the foreign producer was interrupted", ex);
        }
    }

    private static String queue() {
        return String.format("fromforeign-%s", UUID.randomUUID());
    }

    private static void pause(final Duration wait) {
        try {
            Thread.sleep(wait.toMillis());
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("waiting was interrupted", ex);
        }
    }

    private static GenericContainer<?> foreign() {
        return new GenericContainer<>(ForeignScheduleIT.image())
            .withNetwork(ForeignScheduleIT.NETWORK)
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
            ForeignScheduleIT.STORE.getHost(),
            ForeignScheduleIT.STORE.getMappedPort(ForeignScheduleIT.PORT)
        );
    }
}
