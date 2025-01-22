/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.result;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for a result that hands out nothing anybody can change.
 *
 * @since 1.0.1
 */
final class UnchangeableResultTest {

    @Test
    void refusesToBeChangedThroughWhatItHandsOut() {
        final Map<String, Object> values = new TaskResult(UnchangeableResultTest.fields()).asMap();
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> values.put(TaskResult.STATUS, State.FAILURE)
        );
    }

    @Test
    void keepsWhatItHoldsWhenOneMoreFieldIsAskedFor() {
        final TaskResult result = new TaskResult(UnchangeableResultTest.fields());
        result.with(TaskResult.TRACEBACK, "somewhere");
        Assertions.assertEquals(4, result.asMap().size());
    }

    private static Map<String, Object> fields() {
        return Map.of(
            TaskResult.ID, "task-one",
            TaskResult.STATUS, State.SUCCESS,
            TaskResult.VALUE, 42,
            TaskResult.CHILDREN, List.of()
        );
    }
}
