/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.result;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link TaskResult}.
 *
 * @since 0.1.0
 */
final class TaskResultTest {

    /**
     * Identifier used across the cases.
     */
    private static final String ID = "task-one";

    @Test
    void readsTheIdentifier() {
        Assertions.assertEquals(TaskResultTest.ID, TaskResultTest.result().id());
    }

    @Test
    void refusesAResultWithoutIdentifier() {
        Assertions.assertThrows(
            ResultException.class, () -> new TaskResult(Map.of()).id()
        );
    }

    @Test
    void refusesAnIdentifierOfAnotherKind() {
        Assertions.assertThrows(
            ResultException.class, () -> new TaskResult(Map.of(TaskResult.ID, 7)).id()
        );
    }

    @Test
    void readsTheState() {
        Assertions.assertEquals(new State(State.SUCCESS), TaskResultTest.result().state());
    }

    @Test
    void readsAnAbsentStateAsPending() {
        Assertions.assertEquals(
            new State(State.PENDING),
            new TaskResult(Map.of(TaskResult.ID, TaskResultTest.ID)).state()
        );
    }

    @Test
    void refusesAStateOfAnotherKind() {
        Assertions.assertThrows(
            ResultException.class,
            () -> new TaskResult(Map.of(TaskResult.STATUS, 7)).state()
        );
    }

    @Test
    void readsTheValue() {
        Assertions.assertEquals(Optional.of(42), TaskResultTest.result().value());
    }

    @Test
    void readsAnAbsentValueAsEmpty() {
        Assertions.assertEquals(
            Optional.empty(),
            new TaskResult(Map.of(TaskResult.ID, TaskResultTest.ID)).value()
        );
    }

    @Test
    void readsTheTraceback() {
        Assertions.assertEquals(
            Optional.of("Traceback: boom"),
            TaskResultTest.result().with(TaskResult.TRACEBACK, "Traceback: boom").traceback()
        );
    }

    @Test
    void readsANullTracebackAsEmpty() {
        final Map<String, Object> values = new HashMap<>();
        values.put(TaskResult.ID, TaskResultTest.ID);
        values.put(TaskResult.TRACEBACK, null);
        Assertions.assertEquals(Optional.empty(), new TaskResult(values).traceback());
    }

    @Test
    void readsATracebackOfAnotherKindAsEmpty() {
        Assertions.assertEquals(
            Optional.empty(),
            new TaskResult(Map.of(TaskResult.TRACEBACK, 7)).traceback()
        );
    }

    @Test
    void addsAField() {
        Assertions.assertEquals(
            Optional.of("2026-09-19T12:00:00Z"),
            Optional.ofNullable(
                TaskResultTest.result()
                    .with(TaskResult.DONE, "2026-09-19T12:00:00Z")
                    .asMap()
                    .get(TaskResult.DONE)
            )
        );
    }

    @Test
    void leavesTheResultItCameFromAlone() {
        final TaskResult result = TaskResultTest.result();
        result.with(TaskResult.DONE, "2026-09-19T12:00:00Z");
        Assertions.assertFalse(result.asMap().containsKey(TaskResult.DONE));
    }

    @Test
    void refusesToBeModifiedThroughItsMap() {
        final Map<String, Object> values = TaskResultTest.result().asMap();
        Assertions.assertThrows(
            UnsupportedOperationException.class, () -> values.put(TaskResult.DONE, "now")
        );
    }

    @Test
    void comparesEqualByValue() {
        Assertions.assertEquals(TaskResultTest.result(), TaskResultTest.result());
    }

    @Test
    void comparesEqualToItself() {
        final TaskResult result = TaskResultTest.result();
        Assertions.assertEquals(result, result);
    }

    @Test
    void comparesUnequalToAnotherResult() {
        Assertions.assertNotEquals(
            TaskResultTest.result(), TaskResultTest.result().with(TaskResult.VALUE, 43)
        );
    }

    @Test
    void comparesUnequalToAnotherKind() {
        Assertions.assertNotEquals(TaskResultTest.result(), "not a result");
    }

    @Test
    void hashesByValue() {
        Assertions.assertEquals(
            TaskResultTest.result().hashCode(), TaskResultTest.result().hashCode()
        );
    }

    @Test
    void describesItself() {
        Assertions.assertTrue(TaskResultTest.result().toString().contains(TaskResultTest.ID));
    }

    private static TaskResult result() {
        return new TaskResult(
            Map.of(
                TaskResult.ID, TaskResultTest.ID,
                TaskResult.STATUS, State.SUCCESS,
                TaskResult.VALUE, 42
            )
        );
    }
}
