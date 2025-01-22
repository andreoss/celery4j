/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.result;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;

/**
 * Reads and writes results in the JSON shape the store holds them in.
 *
 * @since 0.1.0
 */
public final class JsonResults {

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
     * Mapper the results are read and written with.
     */
    private final ObjectMapper mapper;

    /**
     * Ctor.
     */
    public JsonResults() {
        this(JsonResults.DEFAULT);
    }

    /**
     * Ctor.
     *
     * @param mapper Mapper the results are read and written with
     */
    public JsonResults(final ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * Write a result.
     *
     * @param result Result to write
     * @return The bytes a store holds
     * @throws ResultException If the result cannot be written
     */
    public byte[] encode(final TaskResult result) throws ResultException {
        try {
            return this.mapper.writeValueAsBytes(result.asMap());
        } catch (final JsonProcessingException ex) {
            throw new ResultException("result cannot be written as JSON", ex);
        }
    }

    /**
     * Read a result.
     *
     * @param raw Bytes a store held
     * @return The result those bytes carry
     * @throws ResultException If the bytes are no result
     */
    public TaskResult decode(final byte[] raw) throws ResultException {
        final JsonNode node;
        try {
            node = this.mapper.readTree(raw);
        } catch (final IOException ex) {
            throw new ResultException("result is no JSON", ex);
        }
        if (node == null || !node.isObject()) {
            throw new ResultException("result is no JSON object");
        }
        return new TaskResult(this.mapper.convertValue(node, JsonResults.MAP));
    }
}
