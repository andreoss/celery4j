/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2024
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Reads and writes the body of a protocol version two task message.
 *
 * <p>The body is a three part array of positional arguments, keyword arguments
 * and embedded work-flow fields, serialized as JSON and carried as base64
 * text.</p>
 *
 * @since 0.1.0
 */
public final class JsonTaskBody implements TaskBodyCodec {

    /**
     * Number of parts a body of this protocol version has.
     */
    private static final int PARTS = 3;

    /**
     * Shape a JSON object is read into.
     */
    private static final TypeReference<Map<String, Object>> MAP =
        new TypeReference<Map<String, Object>>() { };

    /**
     * Shape a JSON array is read into.
     */
    private static final TypeReference<List<Object>> LIST =
        new TypeReference<List<Object>>() { };

    /**
     * Mapper used when none was given.
     */
    private static final ObjectMapper DEFAULT = new ObjectMapper();

    /**
     * Mapper the body is read and written with.
     */
    private final ObjectMapper mapper;

    /**
     * Ctor.
     */
    public JsonTaskBody() {
        this(JsonTaskBody.DEFAULT);
    }

    /**
     * Ctor.
     *
     * @param mapper Mapper the body is read and written with
     */
    public JsonTaskBody(final ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public String encode(final TaskBody body) throws ProtocolException {
        final ArrayNode parts = this.mapper.createArrayNode();
        final byte[] json;
        try {
            parts.add(this.mapper.valueToTree(body.args()));
            parts.add(this.mapper.valueToTree(body.kwargs()));
            parts.add(this.mapper.valueToTree(body.embed()));
            json = this.mapper.writeValueAsBytes(parts);
        } catch (final JsonProcessingException ex) {
            throw new ProtocolException("body cannot be written as JSON", ex);
        } catch (final IllegalArgumentException ex) {
            throw new ProtocolException("body holds a value no JSON can hold", ex);
        }
        return Base64.getEncoder().encodeToString(json);
    }

    @Override
    public TaskBody decode(final String body) throws ProtocolException {
        final JsonNode parts = this.parse(JsonTaskBody.unwrap(body));
        if (!parts.isArray() || parts.size() != JsonTaskBody.PARTS) {
            throw new ProtocolException(
                String.format("body is no array of %d parts: %s", JsonTaskBody.PARTS, parts)
            );
        }
        return new TaskBody(
            this.list(parts.get(0)),
            this.map(parts.get(1)),
            this.map(parts.get(2))
        );
    }

    private JsonNode parse(final byte[] json) {
        try {
            return this.mapper.readTree(json);
        } catch (final IOException ex) {
            throw new ProtocolException("body is no JSON", ex);
        }
    }

    private List<Object> list(final JsonNode node) {
        final List<Object> values;
        if (node.isNull()) {
            values = List.of();
        } else if (node.isArray()) {
            values = this.mapper.convertValue(node, JsonTaskBody.LIST);
        } else {
            throw new ProtocolException(String.format("expected an array, found: %s", node));
        }
        return values;
    }

    private Map<String, Object> map(final JsonNode node) {
        final Map<String, Object> values;
        if (node.isNull()) {
            values = Map.of();
        } else if (node.isObject()) {
            values = this.mapper.convertValue(node, JsonTaskBody.MAP);
        } else {
            throw new ProtocolException(String.format("expected an object, found: %s", node));
        }
        return values;
    }

    private static byte[] unwrap(final String body) {
        final byte[] json;
        try {
            json = Base64.getDecoder().decode(body.getBytes(StandardCharsets.UTF_8));
        } catch (final IllegalArgumentException ex) {
            throw new ProtocolException("body is no base64 text", ex);
        }
        return json;
    }
}
