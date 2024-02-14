/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2024
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.client;

import io.celery4j.protocol.ProtocolV2;
import io.celery4j.protocol.Task;
import io.celery4j.result.Backend;
import io.celery4j.result.Results;
import io.celery4j.transport.Broker;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * One call to ask for a task, and a handle to wait on what it returns.
 *
 * <p>This is what a caller needs and nothing more: no message to assemble, no
 * codec to choose, no queue to remember. The queue comes from the routing, the
 * identifier is minted here, and the handle knows where the result will be.</p>
 *
 * <p>Closing a client closes the broker and the store it was given.</p>
 *
 * @since 0.2.0
 */
public final class Client implements AutoCloseable {

    /**
     * Protocol used when none was given.
     */
    private static final ProtocolV2 PROTOCOL = new ProtocolV2();

    /**
     * Routing used when none was given.
     */
    private static final Routes ROUTES = new Routes();

    /**
     * Queue the tasks are sent through.
     */
    private final Broker broker;

    /**
     * Store the results are read from.
     */
    private final Backend backend;

    /**
     * Protocol the messages are written in.
     */
    private final ProtocolV2 protocol;

    /**
     * Which queue a task name goes to.
     */
    private final Routes routes;

    /**
     * Ctor.
     *
     * @param broker Queue the tasks are sent through
     * @param backend Store the results are read from
     */
    public Client(final Broker broker, final Backend backend) {
        this(broker, backend, Client.PROTOCOL, Client.ROUTES);
    }

    /**
     * Ctor.
     *
     * @param broker Queue the tasks are sent through
     * @param backend Store the results are read from
     * @param protocol Protocol the messages are written in
     * @param routes Which queue a task name goes to
     */
    public Client(
        final Broker broker,
        final Backend backend,
        final ProtocolV2 protocol,
        final Routes routes
    ) {
        this.broker = broker;
        this.backend = backend;
        this.protocol = protocol;
        this.routes = routes;
    }

    /**
     * Ask for a task with positional arguments.
     *
     * @param name Name of the task
     * @param args Positional arguments
     * @return A handle on what it returns
     */
    public Handle delay(final String name, final Object... args) {
        return this.call(
            new Task(UUID.randomUUID().toString(), name, List.of(args), Map.of())
        );
    }

    /**
     * Ask for a task with keyword arguments.
     *
     * @param name Name of the task
     * @param kwargs Keyword arguments
     * @return A handle on what it returns
     */
    public Handle delay(final String name, final Map<String, Object> kwargs) {
        return this.call(
            new Task(UUID.randomUUID().toString(), name, List.of(), kwargs)
        );
    }

    /**
     * Ask for a task that is already made up.
     *
     * @param task Task to ask for
     * @return A handle on what it returns
     */
    public Handle call(final Task task) {
        this.broker.send(
            this.protocol.message(task, this.routes.of(task.name())),
            this.routes.of(task.name())
        );
        return new Handle(task.id(), new Results(this.backend));
    }

    /**
     * A handle on a task somebody else asked for.
     *
     * @param id Identifier of the task
     * @return A handle on what it returns
     */
    public Handle handle(final String id) {
        return new Handle(id, new Results(this.backend));
    }

    @Override
    public void close() {
        this.broker.close();
        this.backend.close();
    }
}
