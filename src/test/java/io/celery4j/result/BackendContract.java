/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.result;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * What every result store promises, whichever store it is.
 *
 * @since 0.1.0
 */
interface BackendContract {

    /**
     * A store to test.
     *
     * @return The store
     */
    Backend backend();

    /**
     * The identifier the cases of this run work under.
     *
     * @return Identifier of a task
     */
    String id();

    /**
     * A store hands back what it was given.
     */
    @Test
    default void holdsWhatItWasGiven() {
        try (Backend backend = this.backend()) {
            final TaskResult stored = this.result("kept", State.SUCCESS);
            backend.store(stored);
            Assertions.assertEquals(Optional.of(stored), backend.of(this.id("kept")));
        }
    }

    /**
     * A store knows nothing of a task nobody wrote about.
     */
    @Test
    default void holdsNothingOfAnUnknownTask() {
        try (Backend backend = this.backend()) {
            Assertions.assertEquals(Optional.empty(), backend.of(this.id("unknown")));
        }
    }

    /**
     * A stored result keeps the value its task returned.
     */
    @Test
    default void keepsTheValueOfATask() {
        try (Backend backend = this.backend()) {
            backend.store(this.result("value", State.SUCCESS));
            Assertions.assertEquals(
                Optional.of(42), backend.of(this.id("value")).orElseThrow().value()
            );
        }
    }

    /**
     * A stored result keeps the state its task ended in.
     */
    @Test
    default void keepsTheStateOfATask() {
        try (Backend backend = this.backend()) {
            backend.store(this.result("state", State.FAILURE));
            Assertions.assertEquals(
                new State(State.FAILURE),
                backend.of(this.id("state")).orElseThrow().state()
            );
        }
    }

    /**
     * A later result replaces an earlier one.
     */
    @Test
    default void replacesWhatItHeld() {
        try (Backend backend = this.backend()) {
            backend.store(this.result("twice", State.STARTED));
            backend.store(this.result("twice", State.SUCCESS));
            Assertions.assertEquals(
                new State(State.SUCCESS),
                backend.of(this.id("twice")).orElseThrow().state()
            );
        }
    }

    /**
     * The identifier a case works under.
     *
     * @param suffix What tells this case apart from the others
     * @return Identifier of the task
     */
    default String id(final String suffix) {
        return String.format("%s-%s", this.id(), suffix);
    }

    /**
     * A result of a task.
     *
     * @param suffix What tells this case apart from the others
     * @param state State the task ended in
     * @return The result
     */
    default TaskResult result(final String suffix, final String state) {
        return new TaskResult(
            Map.of(
                TaskResult.ID, this.id(suffix),
                TaskResult.STATUS, state,
                TaskResult.VALUE, 42,
                TaskResult.CHILDREN, List.of()
            )
        );
    }
}
