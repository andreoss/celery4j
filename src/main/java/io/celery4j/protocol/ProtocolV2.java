/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Version two of the task message protocol, where the meta data of a task
 * lives in the headers and only its parameters live in the body.
 *
 * <p>This is the version a worker detects by the presence of a task name in
 * the headers.</p>
 *
 * @since 0.1.0
 */
public final class ProtocolV2 {

    /**
     * Value of the language header for tasks produced here.
     */
    public static final String LANG = "java";

    /**
     * Body codec used when none was given.
     */
    private static final TaskBodyCodec BODIES = new JsonTaskBody();

    /**
     * Name of this node.
     */
    private static final String NODE = ProtocolV2.node();

    /**
     * Codec the body is read and written with.
     */
    private final TaskBodyCodec bodies;

    /**
     * Source of the identifiers a delivery needs.
     */
    private final Supplier<String> ids;

    /**
     * Name of the node producing these messages.
     */
    private final String origin;

    /**
     * Ctor.
     */
    public ProtocolV2() {
        this(ProtocolV2.BODIES, ProtocolV2::uuid, ProtocolV2.NODE);
    }

    /**
     * Ctor.
     *
     * @param bodies Codec the body is read and written with
     * @param ids Source of the identifiers a delivery needs
     * @param origin Name of the node producing these messages
     */
    public ProtocolV2(
        final TaskBodyCodec bodies, final Supplier<String> ids, final String origin
    ) {
        this.bodies = bodies;
        this.ids = ids;
        this.origin = origin;
    }

    /**
     * Build the message that asks for a task to be run.
     *
     * @param task Task to run
     * @param queue Queue the task is routed to
     * @return The message a broker carries
     * @throws ProtocolException If the body cannot be written
     */
    public Message message(final Task task, final String queue) throws ProtocolException {
        return new Message(
            this.properties(task, queue), this.headers(task), this.bodies.encode(task.body())
        );
    }

    /**
     * Read the task a message asks for.
     *
     * @param message Message a broker delivered
     * @return The task that message asks for
     * @throws ProtocolException If the message is no task of this version
     */
    public Task task(final Message message) throws ProtocolException {
        final TaskBody body = this.bodies.decode(message.body());
        return new Task(message.id(), message.task(), body.args(), body.kwargs());
    }

    /**
     * Read the body a message carries.
     *
     * @param message Message a broker delivered
     * @return The body that message carries
     * @throws ProtocolException If the body is no body of this version
     */
    public TaskBody body(final Message message) throws ProtocolException {
        return this.bodies.decode(message.body());
    }

    /**
     * Name of a node, as the protocol reports it.
     *
     * @param host Host the process runs on, which may be unknown
     * @return The process and the host it runs on
     */
    static String node(final String host) {
        String named = host;
        if (named == null || named.isEmpty()) {
            named = "unknown";
        }
        return String.format("%d@%s", ProcessHandle.current().pid(), named);
    }

    private static String node() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (final UnknownHostException ex) {
            host = "";
        }
        return ProtocolV2.node(host);
    }

    private MessageHeaders headers(final Task task) {
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put(MessageHeaders.LANG, ProtocolV2.LANG);
        values.put(MessageHeaders.TASK, task.name());
        values.put(MessageHeaders.ID, task.id());
        values.put(MessageHeaders.ROOT, task.id());
        values.put(MessageHeaders.PARENT, null);
        values.put(MessageHeaders.GROUP, null);
        values.put(MessageHeaders.SHADOW, null);
        values.put(MessageHeaders.ETA, null);
        values.put(MessageHeaders.EXPIRES, null);
        values.put(MessageHeaders.RETRIES, 0);
        values.put(MessageHeaders.TIMELIMIT, Arrays.asList(null, null));
        values.put(MessageHeaders.ORIGIN, this.origin);
        return new MessageHeaders(values);
    }

    private MessageProperties properties(final Task task, final String queue) {
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put(MessageProperties.CORRELATION, task.id());
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

    private static String uuid() {
        return UUID.randomUUID().toString();
    }
}
