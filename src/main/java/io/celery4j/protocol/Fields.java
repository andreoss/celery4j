/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import java.util.Map;
import java.util.Optional;

/**
 * Typed reads of the fields a message carries as plain values.
 *
 * @since 0.1.0
 */
final class Fields {

    /**
     * Ctor.
     */
    private Fields() {
    }

    /**
     * Text held by a field.
     *
     * @param values Fields to read from
     * @param name Name of the field
     * @return The value, empty when the field is absent, null or empty
     * @throws ProtocolException If the field holds something that is no text
     */
    static Optional<String> text(final Map<String, Object> values, final String name)
        throws ProtocolException {
        final Object value = values.get(name);
        final Optional<String> found;
        if (value == null) {
            found = Optional.empty();
        } else if (value instanceof String text) {
            found = Optional.of(text).filter(item -> !item.isEmpty());
        } else {
            throw new ProtocolException(
                String.format("field %s is not text: %s", name, value)
            );
        }
        return found;
    }

    /**
     * Number held by a field.
     *
     * @param values Fields to read from
     * @param name Name of the field
     * @param absent Value to report when the field is absent or null
     * @return The value
     * @throws ProtocolException If the field holds something that is no number
     */
    static long number(final Map<String, Object> values, final String name, final long absent)
        throws ProtocolException {
        final Object value = values.get(name);
        final long found;
        if (value == null) {
            found = absent;
        } else if (value instanceof Number number) {
            found = number.longValue();
        } else {
            throw new ProtocolException(
                String.format("field %s is not a number: %s", name, value)
            );
        }
        return found;
    }
}
