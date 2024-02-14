/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2024
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
public final class ProtocolV2 implements Protocol {

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
     * Codec the body is written with.
     */
    private final TaskBodyCodec bodies;

    /**
     * Codecs the body is read with, by the content type a message declares.
     */
    private final BodyCodecs known;

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
        this(
            bodies,
            new BodyCodecs()
                .with(MessageProperties.JSON, bodies)
                .with(BodyCodecs.JSON, bodies),
            ids,
            origin
        );
    }

    /**
     * Ctor.
     *
     * @param bodies Codec the body is written with
     * @param known Codecs the body is read with, by content type
     * @param ids Source of the identifiers a delivery needs
     * @param origin Name of the node producing these messages
     */
    public ProtocolV2(
        final TaskBodyCodec bodies,
        final BodyCodecs known,
        final Supplier<String> ids,
        final String origin
    ) {
        this.bodies = bodies;
        this.known = known;
        this.ids = ids;
        this.origin = origin;
    }

    @Override
    public Message message(final Task task, final String queue) throws ProtocolException {
        return this.message(task, task.body(), queue);
    }

    /**
     * Write a message for a task, with a body of its own.
     *
     * <p>A task that follows another one carries more than its arguments: the
     * work that comes after it travels in the same body, which is why the body
     * is given here rather than taken from the task.</p>
     *
     * @param task Task to ask for
     * @param body Body the message carries
     * @param queue Queue the message is written to
     * @return The message
     * @throws ProtocolException If the body cannot be written
     */
    public Message message(final Task task, final TaskBody body, final String queue)
        throws ProtocolException {
        return new Message(
            new Deliveries(this.ids).properties(task.id(), queue),
            this.headers(task),
            this.bodies.encode(body)
        );
    }

    @Override
    public Task task(final Message message) throws ProtocolException {
        final TaskBody body = this.body(message);
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
        return this.known.of(message).decode(message.body());
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

    private static String uuid() {
        return UUID.randomUUID().toString();
    }
}
