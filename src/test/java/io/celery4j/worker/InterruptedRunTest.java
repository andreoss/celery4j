/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.worker;

import io.celery4j.result.FakeBackend;
import io.celery4j.transport.FailingPool;
import io.celery4j.transport.FakeBroker;
import io.celery4j.transport.Queues;
import io.celery4j.transport.RedisBroker;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for a fleet whose thread somebody interrupted while it waited for
 * its runs to end.
 *
 * @since 1.0.1
 */
final class InterruptedRunTest {

    @AfterEach
    void clear() {
        Thread.interrupted();
    }

    @Test
    void stopsWaitingForItsRunsWhenTheThreadIsInterrupted() {
        try (Fleet fleet = InterruptedRunTest.fleet()) {
            Thread.currentThread().interrupt();
            Assertions.assertThrows(
                WorkerException.class,
                () -> fleet.run(List.of("quiet"), Duration.ofMillis(50L), 1)
            );
        }
    }

    @Test
    void keepsTheThreadInterruptedAfterwards() {
        try (Fleet fleet = InterruptedRunTest.fleet()) {
            Thread.currentThread().interrupt();
            try {
                fleet.run(List.of("quiet"), Duration.ofMillis(50L), 1);
            } catch (final WorkerException ex) {
                Assertions.assertTrue(Thread.currentThread().isInterrupted());
            }
        }
    }

    @Test
    void raisesWhenARunEndsInAFailureOfItsOwn() {
        try (
            Fleet fleet = new Fleet(
                new Worker(
                    new RedisBroker(new FailingPool()),
                    new FakeBackend(new ConcurrentHashMap<>()),
                    new Registry(Map.of())
                ),
                Executors.newSingleThreadExecutor(),
                new AtomicBoolean(false)
            )
        ) {
            Assertions.assertThrows(
                WorkerException.class,
                () -> fleet.run(List.of("quiet"), Duration.ofMillis(50L), 1)
            );
        }
    }

    private static Fleet fleet() {
        return new Fleet(
            new Worker(
                new FakeBroker(new ConcurrentHashMap<>(), new Queues(), new ConcurrentHashMap<>()),
                new FakeBackend(new ConcurrentHashMap<>()),
                new Registry(Map.of())
            ),
            Executors.newSingleThreadExecutor(),
            new AtomicBoolean(false)
        );
    }
}
