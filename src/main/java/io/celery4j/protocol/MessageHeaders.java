/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2024
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Task headers of a message.
 *
 * <p>Headers are the meta data a worker reads without decoding the body: the
 * task name, its identifier, and the work-flow it belongs to. They are held as
 * they arrived, so a header this version does not know is carried through a
 * decode and encode cycle rather than dropped.</p>
 *
 * <p>The map is read as it was given. Pass one nobody else holds, since this
 * type copies on every read rather than on construction.</p>
 *
 * <p>What it is given is held as given, and what it hands out is a view of
 * that which refuses to be written to. A caller that keeps hold of the
 * collection it passed, and changes it later, changes what this holds; pass
 * one nobody else keeps.</p>
 *
 * @since 0.1.0
 */
public final class MessageHeaders {

    /**
     * Name of the header holding the task identifier.
     */
    public static final String ID = "id";

    /**
     * Name of the header holding the task name.
     */
    public static final String TASK = "task";

    /**
     * Name of the header holding the producer language.
     */
    public static final String LANG = "lang";

    /**
     * Name of the header holding the first task of a work-flow.
     */
    public static final String ROOT = "root_id";

    /**
     * Name of the header holding the calling task of a work-flow.
     */
    public static final String PARENT = "parent_id";

    /**
     * Name of the header holding the group this task belongs to.
     */
    public static final String GROUP = "group";

    /**
     * Name of the header holding the name used in logs.
     */
    public static final String SHADOW = "shadow";

    /**
     * Name of the header holding the node that produced the task.
     */
    public static final String ORIGIN = "origin";

    /**
     * Name of the header holding the number of retries so far.
     */
    public static final String RETRIES = "retries";

    /**
     * Name of the header saying that no result is wanted.
     */
    public static final String IGNORE = "ignore_result";

    /**
     * Name of the header holding the printable form of the arguments.
     */
    public static final String ARGSREPR = "argsrepr";

    /**
     * Name of the header holding the printable form of the keyword arguments.
     */
    public static final String KWARGSREPR = "kwargsrepr";

    /**
     * Name of the header holding the time a task may start at.
     */
    public static final String ETA = "eta";

    /**
     * Name of the header holding the time a task stops being worth running.
     */
    public static final String EXPIRES = "expires";

    /**
     * Name of the header holding the soft and hard time limits.
     */
    public static final String TIMELIMIT = "timelimit";

    /**
     * All header values, in the order they were set or decoded.
     */
    private final Map<String, Object> values;

    /**
     * Ctor.
     *
     * @param values Header values, which must name at least the task and its
     *  identifier
     */
    public MessageHeaders(final Map<String, Object> values) {
        this.values = values;
    }

    /**
     * Identifier of the task.
     *
     * @return The task identifier
     * @throws ProtocolException If the header is absent or is no text
     */
    public String id() throws ProtocolException {
        return this.text(MessageHeaders.ID).orElseThrow(
            () -> new ProtocolException("header id is required")
        );
    }

    /**
     * Name of the task, which is what a worker dispatches on.
     *
     * @return The task name
     * @throws ProtocolException If the header is absent or is no text
     */
    public String task() throws ProtocolException {
        return this.text(MessageHeaders.TASK).orElseThrow(
            () -> new ProtocolException("header task is required")
        );
    }

    /**
     * Text value of a header.
     *
     * @param name Name of the header
     * @return The value, empty when the header is absent, null or empty
     * @throws ProtocolException If the header holds something that is no text
     */
    public Optional<String> text(final String name) throws ProtocolException {
        return Fields.text(this.values, name);
    }

    /**
     * Number held by a header.
     *
     * @param name Name of the header
     * @param absent Value to report when the header is absent or null
     * @return The value
     * @throws ProtocolException If the header holds something that is no number
     */
    public long number(final String name, final long absent) throws ProtocolException {
        return Fields.number(this.values, name, absent);
    }

    /**
     * The same headers with one header set, where a null value is kept as a
     * null header, which is what a producer writes for a field that does not
     * apply.
     *
     * @param name Name of the header
     * @param value Value of the header
     * @return Headers carrying that value
     */
    public MessageHeaders with(final String name, final Object value) {
        final Map<String, Object> extended = new LinkedHashMap<>(this.values);
        extended.put(name, value);
        return new MessageHeaders(extended);
    }

    /**
     * Every header, in the order it was set or decoded.
     *
     * @return A view of them nobody can write through
     */
    public Map<String, Object> asMap() {
        return Collections.unmodifiableMap(this.values);
    }

    @Override
    public boolean equals(final Object other) {
        return this == other
            || other instanceof MessageHeaders headers && this.values.equals(headers.values);
    }

    @Override
    public int hashCode() {
        return this.values.hashCode();
    }

    @Override
    public String toString() {
        return String.format("MessageHeaders(%s)", this.values);
    }
}
