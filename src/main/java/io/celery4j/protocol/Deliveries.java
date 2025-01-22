/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The delivery properties a producer writes, which are the same whichever
 * protocol version carries the task.
 *
 * @since 0.1.0
 */
final class Deliveries {

    /**
     * Source of the identifiers a delivery needs.
     */
    private final Supplier<String> ids;

    /**
     * Ctor.
     *
     * @param ids Source of the identifiers a delivery needs
     */
    Deliveries(final Supplier<String> ids) {
        this.ids = ids;
    }

    /**
     * Properties of a delivery to a queue.
     *
     * @param correlation Identifier the result is correlated by
     * @param queue Queue the task is routed to
     * @return The delivery properties
     */
    MessageProperties properties(final String correlation, final String queue) {
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put(MessageProperties.CORRELATION, correlation);
        values.put(MessageProperties.REPLY, this.ids.get());
        values.put(MessageProperties.TAG, this.ids.get());
        values.put(MessageProperties.WRAPPING, MessageProperties.BASE64);
        values.put(MessageProperties.MODE, MessageProperties.PERSISTENT);
        values.put(MessageProperties.PRIORITY, 0);
        values.put(
            MessageProperties.DELIVERY,
            Map.of(MessageProperties.EXCHANGE, "", MessageProperties.ROUTING, queue)
        );
        values.put(MessageProperties.TYPE, MessageProperties.JSON);
        values.put(MessageProperties.ENCODING, MessageProperties.UTF8);
        return new MessageProperties(values);
    }
}
