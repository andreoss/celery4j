/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.result;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * What a task left behind: the state it ended in, the value it returned or the
 * failure it raised, and the tasks it started.
 *
 * <p>The fields are held as they arrived, so a field this version does not
 * know survives being read and written again. The map is read as it was given,
 * so pass one nobody else holds.</p>
 *
 * @since 0.1.0
 */
public final class TaskResult {

    /**
     * Name of the field holding the identifier of the task.
     */
    public static final String ID = "task_id";

    /**
     * Name of the field holding the state.
     */
    public static final String STATUS = "status";

    /**
     * Name of the field holding the value a task returned.
     */
    public static final String VALUE = "result";

    /**
     * Name of the field holding the text of a failure.
     */
    public static final String TRACEBACK = "traceback";

    /**
     * Name of the field holding the tasks this one started.
     */
    public static final String CHILDREN = "children";

    /**
     * Name of the field holding when the task ended.
     */
    public static final String DONE = "date_done";

    /**
     * All fields, in the order they were set or decoded.
     */
    private final Map<String, Object> values;

    /**
     * Ctor.
     *
     * @param values Fields of the result, which must name at least the task
     *  and the state
     */
    public TaskResult(final Map<String, Object> values) {
        this.values = values;
    }

    /**
     * Identifier of the task this result belongs to.
     *
     * @return The identifier
     * @throws ResultException If the field is absent or is no text
     */
    public String id() throws ResultException {
        final Object value = this.values.get(TaskResult.ID);
        if (!(value instanceof String text) || text.isEmpty()) {
            throw new ResultException(
                String.format("field %s is required, found: %s", TaskResult.ID, value)
            );
        }
        return text;
    }

    /**
     * State the task is in.
     *
     * @return The state, pending when the store knows nothing of the task
     * @throws ResultException If the field is no text
     */
    public State state() throws ResultException {
        final Object value = this.values.get(TaskResult.STATUS);
        final State state;
        if (value == null) {
            state = new State(State.PENDING);
        } else if (value instanceof String text) {
            state = new State(text);
        } else {
            throw new ResultException(
                String.format("field %s is not text: %s", TaskResult.STATUS, value)
            );
        }
        return state;
    }

    /**
     * Value the task returned.
     *
     * @return The value, empty when it returned none or has not finished
     */
    public Optional<Object> value() {
        return Optional.ofNullable(this.values.get(TaskResult.VALUE));
    }

    /**
     * Text of the failure the task raised.
     *
     * @return The text, empty when the task did not fail
     */
    public Optional<String> traceback() {
        return Optional.ofNullable(this.values.get(TaskResult.TRACEBACK))
            .filter(String.class::isInstance)
            .map(String.class::cast);
    }

    /**
     * The same result with one field set.
     *
     * @param name Name of the field
     * @param value Value of the field
     * @return A result carrying that value
     */
    public TaskResult with(final String name, final Object value) {
        final Map<String, Object> extended = new LinkedHashMap<>(this.values);
        extended.put(name, value);
        return new TaskResult(extended);
    }

    /**
     * Every field, in the order it was set or decoded.
     *
     * @return An unmodifiable copy of the fields
     */
    public Map<String, Object> asMap() {
        return Collections.unmodifiableMap(this.values);
    }

    @Override
    public boolean equals(final Object other) {
        return this == other
            || other instanceof TaskResult result && this.values.equals(result.values);
    }

    @Override
    public int hashCode() {
        return this.values.hashCode();
    }

    @Override
    public String toString() {
        return String.format("TaskResult(%s)", this.values);
    }
}
