/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2024
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Reads and writes messages in the JSON envelope a text transport carries.
 *
 * <p>The envelope holds the body, the content type and encoding that say how
 * to read it, the task headers, and the delivery properties. The content type
 * and encoding travel beside the properties on the wire and are read back into
 * them, so that a message keeps one place where its delivery is described.</p>
 *
 * @since 0.1.0
 */
public final class JsonMessageCodec implements MessageCodec {

    /**
     * Name of the envelope field holding the body.
     */
    private static final String BODY = "body";

    /**
     * Name of the envelope field holding the task headers.
     */
    private static final String HEADERS = "headers";

    /**
     * Name of the envelope field holding the delivery properties.
     */
    private static final String PROPERTIES = "properties";

    /**
     * Shape a JSON object is read into.
     */
    private static final TypeReference<Map<String, Object>> MAP =
        new TypeReference<Map<String, Object>>() { };

    /**
     * Mapper used when none was given.
     */
    private static final ObjectMapper DEFAULT = new ObjectMapper();

    /**
     * Mapper the envelope is read and written with.
     */
    private final ObjectMapper mapper;

    /**
     * Ctor.
     */
    public JsonMessageCodec() {
        this(JsonMessageCodec.DEFAULT);
    }

    /**
     * Ctor.
     *
     * @param mapper Mapper the envelope is read and written with
     */
    public JsonMessageCodec(final ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public byte[] encode(final Message message) throws ProtocolException {
        final MessageProperties properties = message.properties();
        final Map<String, Object> rest = new LinkedHashMap<>(properties.asMap());
        rest.remove(MessageProperties.TYPE);
        rest.remove(MessageProperties.ENCODING);
        final ObjectNode envelope = this.mapper.createObjectNode();
        envelope.put(JsonMessageCodec.BODY, message.body());
        envelope.put(
            MessageProperties.ENCODING,
            properties.text(MessageProperties.ENCODING).orElse(MessageProperties.UTF8)
        );
        envelope.put(
            MessageProperties.TYPE,
            properties.text(MessageProperties.TYPE).orElse(MessageProperties.JSON)
        );
        try {
            envelope.set(
                JsonMessageCodec.HEADERS, this.mapper.valueToTree(message.headers().asMap())
            );
            envelope.set(JsonMessageCodec.PROPERTIES, this.mapper.valueToTree(rest));
            return this.mapper.writeValueAsBytes(envelope);
        } catch (final JsonProcessingException ex) {
            throw new ProtocolException("message cannot be written as JSON", ex);
        } catch (final IllegalArgumentException ex) {
            throw new ProtocolException("message holds a value no JSON can hold", ex);
        }
    }

    @Override
    public Message decode(final byte[] raw) throws ProtocolException {
        final JsonNode envelope = this.read(raw);
        final Map<String, Object> properties = this.map(
            JsonMessageCodec.required(envelope, JsonMessageCodec.PROPERTIES)
        );
        properties.put(
            MessageProperties.TYPE,
            JsonMessageCodec.text(envelope, MessageProperties.TYPE)
        );
        properties.put(
            MessageProperties.ENCODING,
            JsonMessageCodec.text(envelope, MessageProperties.ENCODING)
        );
        return new Message(
            new MessageProperties(properties),
            new MessageHeaders(
                this.map(JsonMessageCodec.required(envelope, JsonMessageCodec.HEADERS))
            ),
            JsonMessageCodec.text(envelope, JsonMessageCodec.BODY)
        );
    }

    private JsonNode read(final byte[] raw) {
        final JsonNode envelope;
        try {
            envelope = this.mapper.readTree(raw);
        } catch (final IOException ex) {
            throw new ProtocolException("message is no JSON envelope", ex);
        }
        if (envelope == null || !envelope.isObject()) {
            throw new ProtocolException("message envelope is no JSON object");
        }
        return envelope;
    }

    private Map<String, Object> map(final JsonNode node) {
        if (!node.isObject()) {
            throw new ProtocolException(String.format("expected an object, found: %s", node));
        }
        return this.mapper.convertValue(node, JsonMessageCodec.MAP);
    }

    private static JsonNode required(final JsonNode node, final String name) {
        final JsonNode field = node.get(name);
        if (field == null || field.isNull()) {
            throw new ProtocolException(String.format("field %s is required", name));
        }
        return field;
    }

    private static String text(final JsonNode node, final String name) {
        final JsonNode field = JsonMessageCodec.required(node, name);
        if (!field.isTextual()) {
            throw new ProtocolException(String.format("field %s is not text: %s", name, field));
        }
        return field.textValue();
    }
}
