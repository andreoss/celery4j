/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Delivery properties of a message: what a broker needs to route it and what a
 * consumer needs to read its body.
 *
 * <p>They are held as they arrived, so a property this version does not know
 * survives a decode and encode cycle. The map is read as it was given, so pass
 * one nobody else holds.</p>
 *
 * <p>What it is given is held as given, and what it hands out is a view of
 * that which refuses to be written to. A caller that keeps hold of the
 * collection it passed, and changes it later, changes what this holds; pass
 * one nobody else keeps.</p>
 *
 * @since 0.1.0
 */
public final class MessageProperties {

    /**
     * Name of the property holding the identifier a result is correlated by.
     */
    public static final String CORRELATION = "correlation_id";

    /**
     * Name of the property holding the queue a reply is expected on.
     */
    public static final String REPLY = "reply_to";

    /**
     * Name of the property holding the identifier of a delivery.
     */
    public static final String TAG = "delivery_tag";

    /**
     * Name of the property holding the encoding the body is wrapped in.
     */
    public static final String WRAPPING = "body_encoding";

    /**
     * Name of the property holding the delivery mode.
     */
    public static final String MODE = "delivery_mode";

    /**
     * Name of the property holding the priority within the queue.
     */
    public static final String PRIORITY = "priority";

    /**
     * Name of the property holding the exchange and the routing key.
     */
    public static final String DELIVERY = "delivery_info";

    /**
     * Name of the delivery field holding the exchange.
     */
    public static final String EXCHANGE = "exchange";

    /**
     * Name of the delivery field holding the routing key.
     */
    public static final String ROUTING = "routing_key";

    /**
     * Name of the property holding the mime type of the body.
     */
    public static final String TYPE = "content-type";

    /**
     * Name of the property holding the character encoding of the body.
     */
    public static final String ENCODING = "content-encoding";

    /**
     * Mime type of a body serialized as JSON.
     */
    public static final String JSON = "application/json";

    /**
     * Character encoding of a textual body.
     */
    public static final String UTF8 = "utf-8";

    /**
     * Encoding a body is wrapped in when the transport carries text.
     */
    public static final String BASE64 = "base64";

    /**
     * Delivery mode that asks a broker to persist the message.
     */
    public static final int PERSISTENT = 2;

    /**
     * All property values, in the order they were set or decoded.
     */
    private final Map<String, Object> values;

    /**
     * Ctor.
     *
     * @param values Property values, which must name at least the identifier a
     *  result is correlated by
     */
    public MessageProperties(final Map<String, Object> values) {
        this.values = values;
    }

    /**
     * Identifier the result of this task is correlated by.
     *
     * @return The correlation identifier
     * @throws ProtocolException If the property is absent or is no text
     */
    public String correlation() throws ProtocolException {
        return this.text(MessageProperties.CORRELATION).orElseThrow(
            () -> new ProtocolException("property correlation_id is required")
        );
    }

    /**
     * Text value of a property.
     *
     * @param name Name of the property
     * @return The value, empty when the property is absent, null or empty
     * @throws ProtocolException If the property holds something that is no text
     */
    public Optional<String> text(final String name) throws ProtocolException {
        return Fields.text(this.values, name);
    }

    /**
     * Number held by a property.
     *
     * @param name Name of the property
     * @param absent Value to report when the property is absent or null
     * @return The value
     * @throws ProtocolException If the property holds something that is no number
     */
    public long number(final String name, final long absent) throws ProtocolException {
        return Fields.number(this.values, name, absent);
    }

    /**
     * The same properties with one property set.
     *
     * @param name Name of the property
     * @param value Value of the property
     * @return Properties carrying that value
     */
    public MessageProperties with(final String name, final Object value) {
        final Map<String, Object> extended = new LinkedHashMap<>(this.values);
        extended.put(name, value);
        return new MessageProperties(extended);
    }

    /**
     * Every property, in the order it was set or decoded.
     *
     * @return A view of them nobody can write through
     */
    public Map<String, Object> asMap() {
        return Collections.unmodifiableMap(this.values);
    }

    @Override
    public boolean equals(final Object other) {
        return this == other
            || other instanceof MessageProperties props && this.values.equals(props.values);
    }

    @Override
    public int hashCode() {
        return this.values.hashCode();
    }

    @Override
    public String toString() {
        return String.format("MessageProperties(%s)", this.values);
    }
}
