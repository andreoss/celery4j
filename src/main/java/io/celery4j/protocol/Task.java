/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A task to run: its name, the identifier its result is found under, and the
 * parameters it is called with.
 *
 * <p>Every read hands out a copy, so the call cannot be changed through what
 * it returns.</p>
 *
 * <p>What it is given is held as given, and what it hands out is a view of
 * that which refuses to be written to. A caller that keeps hold of the
 * collection it passed, and changes it later, changes what this holds; pass
 * one nobody else keeps.</p>
 *
 * @since 0.1.0
 */
public final class Task {

    /**
     * Identifier of this call.
     */
    private final String key;

    /**
     * Name a worker dispatches on.
     */
    private final String label;

    /**
     * Positional arguments.
     */
    private final List<Object> positional;

    /**
     * Keyword arguments.
     */
    private final Map<String, Object> keyword;

    /**
     * Ctor.
     *
     * @param key Identifier of this call
     * @param label Name a worker dispatches on
     * @param positional Positional arguments, which may be null for none
     * @param keyword Keyword arguments, which may be null for none
     */
    public Task(
        final String key,
        final String label,
        final List<Object> positional,
        final Map<String, Object> keyword
    ) {
        this.key = key;
        this.label = label;
        this.positional = positional;
        this.keyword = keyword;
    }

    /**
     * Identifier of this call.
     *
     * @return The identifier
     */
    public String id() {
        return this.key;
    }

    /**
     * Name a worker dispatches on.
     *
     * @return The task name
     */
    public String name() {
        return this.label;
    }

    /**
     * Positional arguments.
     *
     * @return A view nobody can write through, empty when none were given
     */
    public List<Object> args() {
        return Task.copied(this.positional);
    }

    /**
     * Keyword arguments.
     *
     * @return A view nobody can write through, empty when none were given
     */
    public Map<String, Object> kwargs() {
        return Task.copied(this.keyword);
    }

    /**
     * The body this call is carried in.
     *
     * @return The body of a message asking for this task
     */
    public TaskBody body() {
        return new TaskBody(this.args(), this.kwargs(), Map.of());
    }

    @Override
    public boolean equals(final Object other) {
        return this == other
            || other instanceof Task task
            && Objects.equals(this.key, task.key)
            && Objects.equals(this.label, task.label)
            && this.args().equals(task.args())
            && this.kwargs().equals(task.kwargs());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.key, this.label, this.args(), this.kwargs());
    }

    @Override
    public String toString() {
        return String.format("Task(%s, %s, %s)", this.key, this.label, this.args());
    }

    private static List<Object> copied(final List<Object> given) {
        final List<Object> view;
        if (given == null) {
            view = List.of();
        } else {
            view = Collections.unmodifiableList(given);
        }
        return view;
    }

    private static Map<String, Object> copied(final Map<String, Object> given) {
        final Map<String, Object> view;
        if (given == null) {
            view = Map.of();
        } else {
            view = Collections.unmodifiableMap(given);
        }
        return view;
    }
}
