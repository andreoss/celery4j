/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2024
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Version one of the task message protocol, where every field of a task lives
 * in the body and the headers are empty.
 *
 * <p>This is the version a worker falls back to when the headers name no
 * task.</p>
 *
 * @since 0.1.0
 */
public final class ProtocolV1 implements Protocol {

    /**
     * Name of the body field holding the task name.
     */
    public static final String TASK = "task";

    /**
     * Name of the body field holding the task identifier.
     */
    public static final String ID = "id";

    /**
     * Name of the body field holding the positional arguments.
     */
    public static final String ARGS = "args";

    /**
     * Name of the body field holding the keyword arguments.
     */
    public static final String KWARGS = "kwargs";

    /**
     * Name of the body field holding the number of retries so far.
     */
    public static final String RETRIES = "retries";

    /**
     * Name of the body field saying the times are in the coordinated zone.
     */
    public static final String UTC = "utc";

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
     * Source of the identifiers a delivery needs.
     */
    private final Supplier<String> ids;

    /**
     * Ctor.
     */
    public ProtocolV1() {
        this(ProtocolV1.DEFAULT, ProtocolV1::uuid);
    }

    /**
     * Ctor.
     *
     * @param mapper Mapper the body is read and written with
     * @param ids Source of the identifiers a delivery needs
     */
    public ProtocolV1(final ObjectMapper mapper, final Supplier<String> ids) {
        this.mapper = mapper;
        this.ids = ids;
    }

    @Override
    public Message message(final Task task, final String queue) throws ProtocolException {
        return new Message(
            new Deliveries(this.ids).properties(task.id(), queue),
            new MessageHeaders(Map.of()),
            this.body(task)
        );
    }

    @Override
    public Task task(final Message message) throws ProtocolException {
        final Map<String, Object> body = this.read(message.body());
        return new Task(
            Fields.text(body, ProtocolV1.ID).orElseThrow(
                () -> new ProtocolException("body field id is required")
            ),
            Fields.text(body, ProtocolV1.TASK).orElseThrow(
                () -> new ProtocolException("body field task is required")
            ),
            this.args(body),
            this.kwargs(body)
        );
    }

    /**
     * Number of times the task of a message has been retried.
     *
     * @param message Message a broker delivered
     * @return The retry count, zero when the body does not say
     * @throws ProtocolException If the body is no body of this version
     */
    public long retries(final Message message) throws ProtocolException {
        return Fields.number(this.read(message.body()), ProtocolV1.RETRIES, 0L);
    }

    private String body(final Task task) {
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put(ProtocolV1.TASK, task.name());
        values.put(ProtocolV1.ID, task.id());
        values.put(ProtocolV1.ARGS, task.args());
        values.put(ProtocolV1.KWARGS, task.kwargs());
        values.put(ProtocolV1.RETRIES, 0);
        values.put(MessageHeaders.ETA, null);
        values.put(MessageHeaders.EXPIRES, null);
        values.put(ProtocolV1.UTC, true);
        values.put(TaskBody.CALLBACKS, null);
        values.put(TaskBody.ERRBACKS, null);
        values.put(TaskBody.CHORD, null);
        values.put(MessageHeaders.TIMELIMIT, Arrays.asList(null, null));
        final byte[] json;
        try {
            json = this.mapper.writeValueAsBytes(values);
        } catch (final JsonProcessingException ex) {
            throw new ProtocolException("body cannot be written as JSON", ex);
        }
        return Base64.getEncoder().encodeToString(json);
    }

    private Map<String, Object> read(final String body) {
        final byte[] json;
        try {
            json = Base64.getDecoder().decode(body.getBytes(StandardCharsets.UTF_8));
        } catch (final IllegalArgumentException ex) {
            throw new ProtocolException("body is no base64 text", ex);
        }
        final JsonNode node;
        try {
            node = this.mapper.readTree(json);
        } catch (final IOException ex) {
            throw new ProtocolException("body is no JSON", ex);
        }
        if (!node.isObject()) {
            throw new ProtocolException(String.format("body is no object: %s", node));
        }
        return this.mapper.convertValue(node, ProtocolV1.MAP);
    }

    private List<Object> args(final Map<String, Object> body) {
        final Object value = body.get(ProtocolV1.ARGS);
        final List<Object> args;
        if (value == null) {
            args = List.of();
        } else if (value instanceof List<?> list) {
            args = this.mapper.convertValue(list, ProtocolV1.LIST);
        } else {
            throw new ProtocolException(String.format("body field args is no array: %s", value));
        }
        return args;
    }

    private Map<String, Object> kwargs(final Map<String, Object> body) {
        final Object value = body.get(ProtocolV1.KWARGS);
        final Map<String, Object> kwargs;
        if (value == null) {
            kwargs = Map.of();
        } else if (value instanceof Map<?, ?> map) {
            kwargs = this.mapper.convertValue(map, ProtocolV1.MAP);
        } else {
            throw new ProtocolException(
                String.format("body field kwargs is no object: %s", value)
            );
        }
        return kwargs;
    }

    private static String uuid() {
        return UUID.randomUUID().toString();
    }
}
