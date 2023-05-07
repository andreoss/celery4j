/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.worker;

import io.celery4j.protocol.Message;
import io.celery4j.protocol.Protocols;
import io.celery4j.protocol.Task;
import io.celery4j.result.Backend;
import io.celery4j.result.State;
import io.celery4j.result.TaskResult;
import io.celery4j.transport.Broker;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * A worker: it takes one message from a queue, runs the task it names, and
 * publishes what that task left behind.
 *
 * <p>A task that raises leaves a failure behind rather than nothing, and a
 * name nobody registered leaves a failure too. Either way the result is
 * published, so that whoever asked for the task learns what became of it.</p>
 *
 * @since 0.1.0
 */
public final class Worker {

    /**
     * Name of the failure field holding the type that was raised.
     */
    public static final String TYPE = "exc_type";

    /**
     * Name of the failure field holding what the type said.
     */
    public static final String MESSAGE = "exc_message";

    /**
     * Name of the failure field holding where the type lives.
     */
    public static final String MODULE = "exc_module";

    /**
     * Protocol versions used when none were given.
     */
    private static final Protocols VERSIONS = new Protocols();

    /**
     * Clock used when none was given.
     */
    private static final Clock UTC = Clock.systemUTC();

    /**
     * Queue the messages come from.
     */
    private final Broker broker;

    /**
     * Store the results go to.
     */
    private final Backend backend;

    /**
     * Names this worker answers for.
     */
    private final Registry registry;

    /**
     * Protocol versions this worker reads.
     */
    private final Protocols protocols;

    /**
     * Clock the results are stamped with.
     */
    private final Clock clock;

    /**
     * Ctor.
     *
     * @param broker Queue the messages come from
     * @param backend Store the results go to
     * @param registry Names this worker answers for
     */
    public Worker(final Broker broker, final Backend backend, final Registry registry) {
        this(broker, backend, registry, Worker.VERSIONS, Worker.UTC);
    }

    /**
     * Ctor.
     *
     * @param broker Queue the messages come from
     * @param backend Store the results go to
     * @param registry Names this worker answers for
     * @param protocols Protocol versions this worker reads
     * @param clock Clock the results are stamped with
     */
    public Worker(
        final Broker broker,
        final Backend backend,
        final Registry registry,
        final Protocols protocols,
        final Clock clock
    ) {
        this.broker = broker;
        this.backend = backend;
        this.registry = registry;
        this.protocols = protocols;
        this.clock = clock;
    }

    /**
     * Take one message and see it through.
     *
     * @param queue Queue to read from
     * @param timeout How long to wait for a message
     * @return The result that was published, empty when no message arrived
     * @throws WorkerException If the message cannot be read as a task
     */
    public Optional<TaskResult> once(final String queue, final Duration timeout)
        throws WorkerException {
        return this.broker.receive(queue, timeout).map(this::handle);
    }

    /**
     * Take messages until a number of them have been seen through, or until
     * the queue stays empty.
     *
     * @param queue Queue to read from
     * @param timeout How long to wait for each message
     * @param count How many to handle at most
     * @return The results that were published, in the order they were
     * @throws WorkerException If a message cannot be read as a task
     */
    public List<TaskResult> some(final String queue, final Duration timeout, final int count)
        throws WorkerException {
        final List<TaskResult> published = new ArrayList<>(count);
        for (int index = 0; index < count; index = index + 1) {
            final Optional<TaskResult> result = this.once(queue, timeout);
            if (result.isEmpty()) {
                break;
            }
            published.add(result.orElseThrow());
        }
        return List.copyOf(published);
    }

    private TaskResult handle(final Message message) {
        final TaskResult result = this.outcome(this.protocols.task(message));
        this.backend.store(result);
        return result;
    }

    private TaskResult outcome(final Task task) {
        return CompletableFuture
            .supplyAsync(() -> this.registry.of(task.name()).run(task), Runnable::run)
            .handle((value, failure) -> this.result(task, value, failure))
            .join();
    }

    private TaskResult result(final Task task, final Object value, final Throwable failure) {
        final TaskResult result;
        if (failure == null) {
            result = this.succeeded(task, value);
        } else {
            result = this.failed(task, Worker.cause(failure));
        }
        return result;
    }

    private TaskResult succeeded(final Task task, final Object value) {
        final Map<String, Object> values = this.fields(task, State.SUCCESS);
        values.put(TaskResult.VALUE, value);
        values.put(TaskResult.TRACEBACK, null);
        return new TaskResult(values);
    }

    private TaskResult failed(final Task task, final Throwable failure) {
        final Map<String, Object> values = this.fields(task, State.FAILURE);
        values.put(TaskResult.VALUE, Worker.raised(failure));
        values.put(TaskResult.TRACEBACK, Worker.text(failure));
        return new TaskResult(values);
    }

    private Map<String, Object> fields(final Task task, final String state) {
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put(TaskResult.ID, task.id());
        values.put(TaskResult.STATUS, state);
        values.put(TaskResult.CHILDREN, List.of());
        values.put(
            TaskResult.DONE,
            DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSS")
                .withZone(ZoneOffset.UTC)
                .format(this.clock.instant())
        );
        return values;
    }

    private static Map<String, Object> raised(final Throwable failure) {
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put(Worker.TYPE, failure.getClass().getSimpleName());
        values.put(Worker.MESSAGE, List.of(String.valueOf(failure.getMessage())));
        values.put(Worker.MODULE, failure.getClass().getPackageName());
        return values;
    }

    private static Throwable cause(final Throwable failure) {
        Throwable found = failure;
        if (failure instanceof CompletionException && failure.getCause() != null) {
            found = failure.getCause();
        }
        return found;
    }

    private static String text(final Throwable failure) {
        final StringWriter written = new StringWriter();
        try (PrintWriter printer = new PrintWriter(written)) {
            failure.printStackTrace(printer);
        }
        return written.toString();
    }
}
