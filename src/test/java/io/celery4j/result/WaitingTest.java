/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.result;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for a wait that somebody cut short, and for a result that cannot
 * be written.
 *
 * @since 1.0.1
 */
final class WaitingTest {

    @AfterEach
    void clear() {
        Thread.interrupted();
    }

    @Test
    void stopsWaitingWhenTheThreadIsInterrupted() {
        Thread.currentThread().interrupt();
        Assertions.assertThrows(
            ResultException.class,
            () -> new Results(new FakeBackend(new ConcurrentHashMap<>()), Duration.ofMillis(50L))
                .await("task-one", Duration.ofSeconds(5L))
        );
    }

    @Test
    void keepsTheThreadInterruptedAfterwards() {
        Thread.currentThread().interrupt();
        try {
            new Results(new FakeBackend(new ConcurrentHashMap<>()), Duration.ofMillis(50L))
                .await("task-one", Duration.ofSeconds(5L));
        } catch (final ResultException ex) {
            Assertions.assertTrue(Thread.currentThread().isInterrupted());
        }
    }

    @Test
    void refusesAResultItCannotWrite() {
        Assertions.assertThrows(
            ResultException.class,
            () -> new JsonResults().encode(new TaskResult(Map.of("value", new Object())))
        );
    }
}
