/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.worker;

import io.celery4j.protocol.Message;
import io.celery4j.protocol.ProtocolV2;
import io.celery4j.protocol.Protocols;
import io.celery4j.protocol.Task;
import io.celery4j.result.FakeBackend;
import io.celery4j.transport.Broker;
import io.celery4j.transport.FakeBroker;
import io.celery4j.transport.Queues;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link Fleet}.
 *
 * @since 0.2.0
 */
final class FleetTest {

    /**
     * Task name used across the cases.
     */
    private static final String NAME = "proj.tasks.add";

    /**
     * Queue used across the cases.
     */
    private static final String QUEUE = "important";

    /**
     * How long a receive waits in these cases.
     */
    private static final Duration WAIT = Duration.ofMillis(20L);

    @Test
    void runsUntilItIsToldToStop() {
        try (
            Fleet fleet = new Fleet(
                FleetTest.worker(FleetTest.broker(), task -> 4),
                Executors.newFixedThreadPool(2),
                new AtomicBoolean()
            )
        ) {
            FleetTest.stopAfter(fleet, Duration.ofMillis(200L));
            Assertions.assertEquals(0L, fleet.run(List.of(FleetTest.QUEUE), FleetTest.WAIT, 2));
        }
    }

    @Test
    void seesEveryMessageThrough() {
        final Broker broker = FleetTest.broker();
        for (int index = 0; index < 8; index = index + 1) {
            broker.send(FleetTest.message(String.format("task-%d", index)), FleetTest.QUEUE);
        }
        try (
            Fleet fleet = new Fleet(
                FleetTest.worker(broker, task -> 4),
                Executors.newFixedThreadPool(2),
                new AtomicBoolean()
            )
        ) {
            FleetTest.stopAfter(fleet, Duration.ofMillis(600L));
            Assertions.assertEquals(8L, fleet.run(List.of(FleetTest.QUEUE), FleetTest.WAIT, 2));
        }
    }

    @Test
    void neverRunsMoreTasksThanItWasAllowed() {
        final Broker broker = FleetTest.broker();
        for (int index = 0; index < 12; index = index + 1) {
            broker.send(FleetTest.message(String.format("task-%d", index)), FleetTest.QUEUE);
        }
        final AtomicInteger running = new AtomicInteger();
        final AtomicInteger most = new AtomicInteger();
        try (
            Fleet fleet = new Fleet(
                FleetTest.worker(broker, FleetTest.counting(running, most)),
                Executors.newFixedThreadPool(3),
                new AtomicBoolean()
            )
        ) {
            FleetTest.stopAfter(fleet, Duration.ofMillis(700L));
            fleet.run(List.of(FleetTest.QUEUE), FleetTest.WAIT, 3);
            Assertions.assertTrue(most.get() <= 3);
        }
    }

    @Test
    void reportsThatItIsStopping() {
        try (
            Fleet fleet = new Fleet(
                FleetTest.worker(FleetTest.broker(), task -> 4),
                Executors.newSingleThreadExecutor(),
                new AtomicBoolean()
            )
        ) {
            fleet.stop();
            Assertions.assertTrue(fleet.stopping());
        }
    }

    @Test
    void reportsThatItIsNotStoppingYet() {
        try (
            Fleet fleet = new Fleet(
                FleetTest.worker(FleetTest.broker(), task -> 4),
                Executors.newSingleThreadExecutor(),
                new AtomicBoolean()
            )
        ) {
            Assertions.assertFalse(fleet.stopping());
        }
    }

    @Test
    void stopsWhenItIsClosed() {
        final Fleet fleet = new Fleet(
            FleetTest.worker(FleetTest.broker(), task -> 4),
            Executors.newSingleThreadExecutor(),
            new AtomicBoolean()
        );
        fleet.close();
        Assertions.assertTrue(fleet.stopping());
    }

    @Test
    void raisesWhatARunRaised() {
        try (
            Fleet fleet = new Fleet(
                new Worker(
                    null, new FakeBackend(new ConcurrentHashMap<>()), new Registry(Map.of())
                ),
                Executors.newSingleThreadExecutor(),
                new AtomicBoolean()
            )
        ) {
            Assertions.assertThrows(
                WorkerException.class,
                () -> fleet.run(List.of(FleetTest.QUEUE), FleetTest.WAIT, 1)
            );
        }
    }

    private static Job counting(final AtomicInteger running, final AtomicInteger most) {
        return task -> {
            final int now = running.incrementAndGet();
            most.accumulateAndGet(now, Math::max);
            try {
                Thread.sleep(40L);
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            running.decrementAndGet();
            return now;
        };
    }

    private static void stopAfter(final Fleet fleet, final Duration delay) {
        final Thread clock = new Thread(
            () -> {
                try {
                    Thread.sleep(delay.toMillis());
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
                fleet.stop();
            }
        );
        clock.setDaemon(true);
        clock.start();
    }

    private static Worker worker(final Broker broker, final Job job) {
        return new Worker(
            broker,
            new FakeBackend(new ConcurrentHashMap<>()),
            new Registry(Map.of(FleetTest.NAME, job)),
            new Options(new Protocols(), Clock.systemUTC(), new RetryPolicy(), null)
        );
    }

    private static Broker broker() {
        return new FakeBroker(new ConcurrentHashMap<>(), new Queues(), new ConcurrentHashMap<>());
    }

    private static Message message(final String id) {
        return new ProtocolV2().message(
            new Task(id, FleetTest.NAME, List.of(2, 2), Map.of()), FleetTest.QUEUE
        );
    }
}
