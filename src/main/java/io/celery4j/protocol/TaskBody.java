/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The decoded body of a task message in protocol version two: positional
 * arguments, keyword arguments, and the embedded work-flow fields.
 *
 * <p>A part that was not given is read as an empty one, which is how a
 * producer that omits it is understood. Every read hands out a copy, so the
 * body cannot be changed through what it returns.</p>
 *
 * @since 0.1.0
 */
public final class TaskBody {

    /**
     * Name of the embedded field holding the tasks to call on success.
     */
    public static final String CALLBACKS = "callbacks";

    /**
     * Name of the embedded field holding the tasks to call on failure.
     */
    public static final String ERRBACKS = "errbacks";

    /**
     * Name of the embedded field holding the rest of the chain.
     */
    public static final String CHAIN = "chain";

    /**
     * Name of the embedded field holding the callback of a chord.
     */
    public static final String CHORD = "chord";

    /**
     * Positional arguments.
     */
    private final List<Object> positional;

    /**
     * Keyword arguments.
     */
    private final Map<String, Object> keyword;

    /**
     * Embedded work-flow fields.
     */
    private final Map<String, Object> workflow;

    /**
     * Ctor.
     *
     * @param positional Positional arguments, which may be null for none
     * @param keyword Keyword arguments, which may be null for none
     * @param workflow Embedded work-flow fields, which may be null for none
     */
    public TaskBody(
        final List<Object> positional,
        final Map<String, Object> keyword,
        final Map<String, Object> workflow
    ) {
        this.positional = positional;
        this.keyword = keyword;
        this.workflow = workflow;
    }

    /**
     * Positional arguments.
     *
     * @return An unmodifiable copy, empty when none were given
     */
    public List<Object> args() {
        final List<Object> copy;
        if (this.positional == null) {
            copy = List.of();
        } else {
            copy = Collections.unmodifiableList(new ArrayList<>(this.positional));
        }
        return copy;
    }

    /**
     * Keyword arguments.
     *
     * @return An unmodifiable copy, empty when none were given
     */
    public Map<String, Object> kwargs() {
        return TaskBody.copy(this.keyword);
    }

    /**
     * Embedded work-flow fields.
     *
     * @return An unmodifiable copy, empty when none were given
     */
    public Map<String, Object> embed() {
        return TaskBody.copy(this.workflow);
    }

    /**
     * An embedded work-flow field, which a producer that is part of no
     * work-flow leaves out or writes as null.
     *
     * @param name Name of the field
     * @return The field, empty when it is absent or null
     */
    public Optional<Object> embedded(final String name) {
        final Optional<Object> found;
        if (this.workflow == null) {
            found = Optional.empty();
        } else {
            found = Optional.ofNullable(this.workflow.get(name));
        }
        return found;
    }

    /**
     * The same body with one more keyword argument.
     *
     * @param name Name of the argument
     * @param value Value of the argument
     * @return A body carrying that argument
     */
    public TaskBody with(final String name, final Object value) {
        final Map<String, Object> extended = new LinkedHashMap<>(this.kwargs());
        extended.put(name, value);
        return new TaskBody(this.positional, extended, this.workflow);
    }

    @Override
    public boolean equals(final Object other) {
        return this == other
            || other instanceof TaskBody body
            && this.args().equals(body.args())
            && this.kwargs().equals(body.kwargs())
            && this.embed().equals(body.embed());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.args(), this.kwargs(), this.embed());
    }

    @Override
    public String toString() {
        return String.format(
            "TaskBody(%s, %s, %s)", this.args(), this.kwargs(), this.embed()
        );
    }

    private static Map<String, Object> copy(final Map<String, Object> values) {
        final Map<String, Object> copy;
        if (values == null) {
            copy = Map.of();
        } else {
            copy = Collections.unmodifiableMap(new LinkedHashMap<>(values));
        }
        return copy;
    }
}
