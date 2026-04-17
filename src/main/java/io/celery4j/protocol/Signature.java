/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * A call a message names for later, as the protocol writes one.
 *
 * <p>The work-flow fields of a body hold calls rather than tasks: a name, the
 * arguments already fixed, and options that say where the call goes and under
 * which identity. What a finished task hands to the next one is put in front
 * of the arguments already there, unless the call says it wants none of
 * that.</p>
 *
 * <p>What it is given is held as given, and what it hands out is a view of
 * that which refuses to be written to. A caller that keeps hold of the
 * collection it passed, and changes it later, changes what this holds; pass
 * one nobody else keeps.</p>
 *
 * @since 1.0.0
 */
public final class Signature {

    /**
     * Name of the field holding the task name.
     */
    public static final String TASK = "task";

    /**
     * Name of the field holding the positional arguments.
     */
    public static final String ARGS = "args";

    /**
     * Name of the field holding the keyword arguments.
     */
    public static final String KWARGS = "kwargs";

    /**
     * Name of the field holding the options of the call.
     */
    public static final String OPTIONS = "options";

    /**
     * Name of the field saying the call wants nothing put in front of its
     * arguments.
     */
    public static final String IMMUTABLE = "immutable";

    /**
     * Name of the option holding the queue the call goes to.
     */
    public static final String QUEUE = "queue";

    /**
     * Name of the option holding the identity the call runs under.
     */
    public static final String ID = "task_id";

    /**
     * Fields of this call.
     */
    private final Map<String, Object> values;

    /**
     * Ctor.
     *
     * @param values Fields of this call
     */
    public Signature(final Map<String, Object> values) {
        this.values = values;
    }

    /**
     * Name the call dispatches on.
     *
     * @return The task name
     * @throws ProtocolException If the call names no task
     */
    public String name() throws ProtocolException {
        final Object value = this.values.get(Signature.TASK);
        if (!(value instanceof String text)) {
            throw new ProtocolException(
                String.format("signature field task is no text: %s", value)
            );
        }
        return text;
    }

    /**
     * Identity the call runs under, invented here when the call names none.
     *
     * @return The identifier
     */
    public String id() {
        final Object value = this.options().get(Signature.ID);
        final String found;
        if (value instanceof String text) {
            found = text;
        } else {
            found = UUID.randomUUID().toString();
        }
        return found;
    }

    /**
     * Positional arguments the call already carries.
     *
     * @return An unmodifiable copy, empty when none were given
     */
    public List<Object> args() {
        return Signature.list(this.values.get(Signature.ARGS));
    }

    /**
     * Keyword arguments the call already carries.
     *
     * @return An unmodifiable copy, empty when none were given
     */
    public Map<String, Object> kwargs() {
        return Signature.map(this.values.get(Signature.KWARGS));
    }

    /**
     * Options of the call.
     *
     * @return An unmodifiable copy, empty when none were given
     */
    public Map<String, Object> options() {
        return Signature.map(this.values.get(Signature.OPTIONS));
    }

    /**
     * Queue the call goes to, when it names one.
     *
     * @return The queue, or empty when the call names none
     */
    public Optional<String> queue() {
        final Object value = this.options().get(Signature.QUEUE);
        final Optional<String> found;
        if (value instanceof String text) {
            found = Optional.of(text);
        } else {
            found = Optional.empty();
        }
        return found;
    }

    /**
     * Whether the call wants nothing put in front of its arguments.
     *
     * @return True when what came before is left out
     */
    public boolean immutable() {
        return Boolean.TRUE.equals(this.values.get(Signature.IMMUTABLE));
    }

    /**
     * The call as a task, with what came before in front of its arguments.
     *
     * @param before What the task before handed over
     * @return The task this call asks for
     * @throws ProtocolException If the call names no task
     */
    public Task task(final Object before) throws ProtocolException {
        final List<Object> args = new ArrayList<>(this.args().size() + 1);
        if (!this.immutable()) {
            args.add(before);
        }
        args.addAll(this.args());
        return new Task(this.id(), this.name(), args, this.kwargs());
    }

    /**
     * Fields of this call.
     *
     * @return An unmodifiable copy
     */
    public Map<String, Object> asMap() {
        return Collections.unmodifiableMap(this.values);
    }

    private static List<Object> list(final Object value) {
        final List<Object> found;
        if (value instanceof List<?> items) {
            found = Collections.unmodifiableList(new ArrayList<>(items));
        } else {
            found = List.of();
        }
        return found;
    }

    private static Map<String, Object> map(final Object value) {
        final Map<String, Object> found = new LinkedHashMap<>();
        if (value instanceof Map<?, ?> fields) {
            for (final Map.Entry<?, ?> entry : fields.entrySet()) {
                found.put(String.valueOf(entry.getKey()), entry.getValue());
            }
        }
        return Collections.unmodifiableMap(found);
    }
}
