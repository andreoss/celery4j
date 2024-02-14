/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2024
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.result;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link Results}.
 *
 * @since 0.1.0
 */
final class ResultsTest {

    /**
     * Identifier used across the cases.
     */
    private static final String ID = "task-one";

    /**
     * How long the cases wait for a result that is already there.
     */
    private static final Duration WAIT = Duration.ofSeconds(1L);

    /**
     * How long the cases wait for a result that never comes.
     */
    private static final Duration GLANCE = Duration.ofMillis(120L);

    @Test
    void handsBackAFinishedResult() {
        try (Backend backend = ResultsTest.holding(State.SUCCESS)) {
            Assertions.assertEquals(
                Optional.of(new State(State.SUCCESS)),
                new Results(backend, Duration.ofMillis(10L))
                    .await(ResultsTest.ID, ResultsTest.WAIT)
                    .map(TaskResult::state)
            );
        }
    }

    @Test
    void reportsATaskThatNeverFinished() {
        try (Backend backend = ResultsTest.holding(State.STARTED)) {
            Assertions.assertEquals(
                Optional.empty(),
                new Results(backend, Duration.ofMillis(10L))
                    .await(ResultsTest.ID, ResultsTest.GLANCE)
            );
        }
    }

    @Test
    void reportsATaskTheStoreKnowsNothingOf() {
        try (Backend backend = new FakeBackend(new ConcurrentHashMap<>())) {
            Assertions.assertEquals(
                Optional.empty(),
                new Results(backend, Duration.ofMillis(10L))
                    .await(ResultsTest.ID, ResultsTest.GLANCE)
            );
        }
    }

    @Test
    void handsBackTheValueOfATask() {
        try (Backend backend = ResultsTest.holding(State.SUCCESS)) {
            Assertions.assertEquals(
                Optional.of(42),
                new Results(backend, Duration.ofMillis(10L))
                    .value(ResultsTest.ID, ResultsTest.WAIT)
            );
        }
    }

    @Test
    void raisesTheFailureOfATask() {
        try (Backend backend = ResultsTest.holding(State.FAILURE)) {
            Assertions.assertThrows(
                ResultException.class,
                () -> new Results(backend, Duration.ofMillis(10L))
                    .value(ResultsTest.ID, ResultsTest.WAIT)
            );
        }
    }

    @Test
    void raisesWhenTheTimeRanOut() {
        try (Backend backend = ResultsTest.holding(State.STARTED)) {
            Assertions.assertThrows(
                ResultException.class,
                () -> new Results(backend, Duration.ofMillis(10L))
                    .value(ResultsTest.ID, ResultsTest.GLANCE)
            );
        }
    }

    @Test
    void asksTheStoreAsOftenAsItWasTold() {
        try (Backend backend = ResultsTest.holding(State.SUCCESS)) {
            Assertions.assertTrue(
                new Results(backend).await(ResultsTest.ID, ResultsTest.WAIT).isPresent()
            );
        }
    }

    private static Backend holding(final String state) {
        final Backend backend = new FakeBackend(new ConcurrentHashMap<>());
        backend.store(
            new TaskResult(
                Map.of(
                    TaskResult.ID, ResultsTest.ID,
                    TaskResult.STATUS, state,
                    TaskResult.VALUE, 42
                )
            )
        );
        return backend;
    }
}
