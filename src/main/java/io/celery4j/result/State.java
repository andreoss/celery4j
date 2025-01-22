/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.result;

import java.util.Objects;
import java.util.Set;

/**
 * The state a task is in.
 *
 * <p>The states this protocol names are known here, and a state it does not
 * name is carried as it arrived: a worker may define its own, and a state
 * nobody here understands is still a state, not an error.</p>
 *
 * @since 0.1.0
 */
public final class State {

    /**
     * Name of the state of a task nothing is known about.
     */
    public static final String PENDING = "PENDING";

    /**
     * Name of the state of a task a worker has taken.
     */
    public static final String RECEIVED = "RECEIVED";

    /**
     * Name of the state of a task a worker is running.
     */
    public static final String STARTED = "STARTED";

    /**
     * Name of the state of a task that returned a value.
     */
    public static final String SUCCESS = "SUCCESS";

    /**
     * Name of the state of a task that raised.
     */
    public static final String FAILURE = "FAILURE";

    /**
     * Name of the state of a task waiting to be run again.
     */
    public static final String RETRY = "RETRY";

    /**
     * Name of the state of a task that was called off.
     */
    public static final String REVOKED = "REVOKED";

    /**
     * States after which nothing more happens to a task.
     */
    private static final Set<String> TERMINAL =
        Set.of(State.SUCCESS, State.FAILURE, State.REVOKED);

    /**
     * Name of this state.
     */
    private final String label;

    /**
     * Ctor.
     *
     * @param label Name of the state
     */
    public State(final String label) {
        this.label = label;
    }

    /**
     * Name of this state.
     *
     * @return The name
     */
    public String name() {
        return this.label;
    }

    /**
     * Whether the task returned a value.
     *
     * @return True when it did
     */
    public boolean successful() {
        return State.SUCCESS.equals(this.label);
    }

    /**
     * Whether the task raised.
     *
     * @return True when it did
     */
    public boolean failed() {
        return State.FAILURE.equals(this.label);
    }

    /**
     * Whether nothing more will happen to the task.
     *
     * @return True when the state is final
     */
    public boolean terminal() {
        return State.TERMINAL.contains(this.label);
    }

    @Override
    public boolean equals(final Object other) {
        return this == other
            || other instanceof State state && Objects.equals(this.label, state.label);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.label);
    }

    @Override
    public String toString() {
        return String.valueOf(this.label);
    }
}
