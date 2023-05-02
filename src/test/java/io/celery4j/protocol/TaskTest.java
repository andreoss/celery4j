/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link Task}.
 *
 * @since 0.1.0
 */
final class TaskTest {

    /**
     * Identifier used across the cases.
     */
    private static final String ID = "task-one";

    /**
     * Task name used across the cases.
     */
    private static final String NAME = "proj.tasks.add";

    @Test
    void readsItsPositionalArguments() {
        Assertions.assertEquals(List.of(2, 2), TaskTest.task().args());
    }

    @Test
    void readsItsKeywordArguments() {
        Assertions.assertEquals(Map.of("debug", true), TaskTest.task().kwargs());
    }

    @Test
    void readsAbsentPositionalArgumentsAsEmpty() {
        Assertions.assertEquals(
            List.of(), new Task(TaskTest.ID, TaskTest.NAME, null, null).args()
        );
    }

    @Test
    void readsAbsentKeywordArgumentsAsEmpty() {
        Assertions.assertEquals(
            Map.of(), new Task(TaskTest.ID, TaskTest.NAME, null, null).kwargs()
        );
    }

    @Test
    void refusesToHaveItsArgumentsModified() {
        final List<Object> args = TaskTest.task().args();
        Assertions.assertThrows(UnsupportedOperationException.class, () -> args.add(3));
    }

    @Test
    void refusesToHaveItsKeywordArgumentsModified() {
        final Map<String, Object> kwargs = TaskTest.task().kwargs();
        Assertions.assertThrows(
            UnsupportedOperationException.class, () -> kwargs.put("more", true)
        );
    }

    @Test
    void carriesItsArgumentsIntoTheBody() {
        Assertions.assertEquals(List.of(2, 2), TaskTest.task().body().args());
    }

    @Test
    void carriesItsKeywordArgumentsIntoTheBody() {
        Assertions.assertEquals(Map.of("debug", true), TaskTest.task().body().kwargs());
    }

    @Test
    void comparesEqualByValue() {
        Assertions.assertEquals(TaskTest.task(), TaskTest.task());
    }

    @Test
    void comparesUnequalToAnotherTask() {
        Assertions.assertNotEquals(
            TaskTest.task(), new Task(TaskTest.ID, "proj.tasks.mul", List.of(), Map.of())
        );
    }

    @Test
    void namesItself() {
        Assertions.assertEquals(TaskTest.NAME, TaskTest.task().name());
    }

    @Test
    void identifiesItself() {
        Assertions.assertEquals(TaskTest.ID, TaskTest.task().id());
    }

    @Test
    void comparesEqualToItself() {
        final Task task = TaskTest.task();
        Assertions.assertEquals(task, task);
    }

    @Test
    void comparesUnequalToAnotherIdentifier() {
        Assertions.assertNotEquals(
            TaskTest.task(),
            new Task("task-two", TaskTest.NAME, List.of(2, 2), Map.of("debug", true))
        );
    }

    @Test
    void comparesUnequalToOtherArguments() {
        Assertions.assertNotEquals(
            TaskTest.task(),
            new Task(TaskTest.ID, TaskTest.NAME, List.of(3), Map.of("debug", true))
        );
    }

    @Test
    void comparesUnequalToOtherKeywordArguments() {
        Assertions.assertNotEquals(
            TaskTest.task(),
            new Task(TaskTest.ID, TaskTest.NAME, List.of(2, 2), Map.of())
        );
    }

    @Test
    void comparesUnequalToAnotherKind() {
        Assertions.assertNotEquals(TaskTest.task(), "not a task");
    }

    @Test
    void hashesByValue() {
        Assertions.assertEquals(TaskTest.task().hashCode(), TaskTest.task().hashCode());
    }

    @Test
    void describesItself() {
        Assertions.assertTrue(TaskTest.task().toString().contains(TaskTest.NAME));
    }

    private static Task task() {
        return new Task(TaskTest.ID, TaskTest.NAME, List.of(2, 2), Map.of("debug", true));
    }
}
