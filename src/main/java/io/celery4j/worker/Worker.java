/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.worker;

import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageHeaders;
import io.celery4j.protocol.MessageProperties;
import io.celery4j.protocol.Schedule;
import io.celery4j.protocol.Task;
import io.celery4j.result.Backend;
import io.celery4j.result.State;
import io.celery4j.result.TaskResult;
import io.celery4j.transport.Broker;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A worker: it takes one message from a queue, runs the task it names, and
 * publishes what that task left behind.
 *
 * <p>A task that raises leaves a failure behind rather than nothing, and a
 * name nobody registered leaves a failure too. Either way the result is
 * published, so that whoever asked for the task learns what became of it.</p>
 *
 * <p>A task whose start time has not come is sent back to the queue untouched,
 * and one that stopped being worth running is recorded as called off without
 * being run at all.</p>
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
     * Options used when none were given.
     */
    private static final Options DEFAULTS = new Options();

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
     * What this worker decides for itself.
     */
    private final Options options;

    /**
     * Ctor.
     *
     * @param broker Queue the messages come from
     * @param backend Store the results go to
     * @param registry Names this worker answers for
     */
    public Worker(final Broker broker, final Backend backend, final Registry registry) {
        this(broker, backend, registry, Worker.DEFAULTS);
    }

    /**
     * Ctor.
     *
     * @param broker Queue the messages come from
     * @param backend Store the results go to
     * @param registry Names this worker answers for
     * @param options What this worker decides for itself
     */
    public Worker(
        final Broker broker,
        final Backend backend,
        final Registry registry,
        final Options options
    ) {
        this.broker = broker;
        this.backend = backend;
        this.registry = registry;
        this.options = options;
    }

    /**
     * Take one message and see it through.
     *
     * @param queue Queue to read from
     * @param timeout How long to wait for a message
     * @return The result that was published, empty when no message arrived or
     *  when the one that did is not due yet
     * @throws WorkerException If the message cannot be read as a task
     */
    public Optional<TaskResult> once(final String queue, final Duration timeout)
        throws WorkerException {
        return this.once(List.of(queue), timeout);
    }

    /**
     * Take one message from the first of these queues that has one and see it
     * through.
     *
     * @param queues Queues to read from, in the order they are served
     * @param timeout How long to wait for a message
     * @return The result that was published, empty when no message arrived or
     *  when the one that did is not due yet
     * @throws WorkerException If the message cannot be read as a task
     */
    public Optional<TaskResult> once(final List<String> queues, final Duration timeout)
        throws WorkerException {
        return this.broker.receive(queues, timeout)
            .flatMap(message -> this.handle(message, Worker.routing(message, queues)));
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

    private Optional<TaskResult> handle(final Message message, final String queue) {
        final Schedule schedule = new Schedule(message.headers());
        final Instant now = this.options.clock().instant();
        final Optional<TaskResult> published;
        if (schedule.expired(now)) {
            published = Optional.of(
                this.store(
                    message,
                    this.ended(this.options.protocols().task(message), State.REVOKED)
                )
            );
        } else if (schedule.due(now)) {
            published = Optional.of(
                this.store(
                    message,
                    this.outcome(
                        new Envelope(message, queue, this.options.protocols().task(message))
                    )
                )
            );
        } else {
            this.broker.send(message, queue);
            published = Optional.empty();
        }
        return published;
    }

    private TaskResult store(final Message message, final TaskResult result) {
        if (!Worker.ignored(message)) {
            this.backend.store(result);
        }
        return result;
    }

    private TaskResult outcome(final Envelope envelope) {
        final TaskResult result = this.running(envelope)
            .handle((value, failure) -> this.result(envelope, value, failure))
            .join();
        if (State.RETRY.equals(result.state().name())) {
            this.broker.send(this.again(envelope.message()), envelope.queue());
        }
        return result;
    }

    private CompletableFuture<Object> running(final Envelope envelope) {
        final Optional<Duration> limit = this.options.limit(envelope.message());
        final CompletableFuture<Object> started;
        if (limit.isEmpty()) {
            started = CompletableFuture.supplyAsync(() -> this.job(envelope), Runnable::run);
        } else {
            started = this.bounded(envelope, limit.orElseThrow());
        }
        return started;
    }

    private CompletableFuture<Object> bounded(final Envelope envelope, final Duration limit) {
        final ExecutorService thread = Executors.newSingleThreadExecutor();
        return CompletableFuture
            .supplyAsync(() -> this.job(envelope), thread)
            .orTimeout(limit.toMillis(), TimeUnit.MILLISECONDS)
            .whenComplete((value, failure) -> thread.shutdownNow());
    }

    private Object job(final Envelope envelope) {
        return this.registry.of(envelope.task().name()).run(envelope.task());
    }

    private TaskResult result(
        final Envelope envelope, final Object value, final Throwable failure
    ) {
        final TaskResult result;
        if (failure == null) {
            result = this.succeeded(envelope.task(), value);
        } else {
            result = this.raised(envelope, Worker.cause(failure));
        }
        return result;
    }

    private TaskResult raised(final Envelope envelope, final Throwable failure) {
        final String state;
        if (failure instanceof RetryException && this.options.retriable(envelope.message())) {
            state = State.RETRY;
        } else {
            state = State.FAILURE;
        }
        return this.ended(envelope.task(), state)
            .with(TaskResult.VALUE, Worker.described(failure))
            .with(TaskResult.TRACEBACK, Worker.text(failure));
    }

    private Message again(final Message message) {
        if (message.headers().text(MessageHeaders.TASK).isEmpty()) {
            throw new WorkerException("a task can only be retried under protocol two");
        }
        final long retries = message.headers().number(MessageHeaders.RETRIES, 0L);
        return new Message(
            message.properties(),
            new Schedule(
                message.headers().with(MessageHeaders.RETRIES, (int) retries + 1)
            ).at(this.options.clock().instant().plus(this.options.retries().delay(retries))),
            message.body()
        );
    }

    private TaskResult succeeded(final Task task, final Object value) {
        return this.ended(task, State.SUCCESS)
            .with(TaskResult.VALUE, value)
            .with(TaskResult.TRACEBACK, null);
    }

    private TaskResult ended(final Task task, final String state) {
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put(TaskResult.ID, task.id());
        values.put(TaskResult.STATUS, state);
        values.put(TaskResult.CHILDREN, List.of());
        values.put(
            TaskResult.DONE,
            DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSS")
                .withZone(ZoneOffset.UTC)
                .format(this.options.clock().instant())
        );
        return new TaskResult(values);
    }

    private static boolean ignored(final Message message) {
        return message.headers().asMap().get(MessageHeaders.IGNORE) instanceof Boolean ignore
            && ignore;
    }

    private static String routing(final Message message, final List<String> queues) {
        String name = queues.get(0);
        final Object delivery = message.properties().asMap().get(MessageProperties.DELIVERY);
        if (delivery instanceof Map<?, ?> info
            && info.get(MessageProperties.ROUTING) instanceof String routed) {
            name = routed;
        }
        return name;
    }

    private static Throwable cause(final Throwable failure) {
        Throwable found = failure;
        if (failure instanceof CompletionException && failure.getCause() != null) {
            found = failure.getCause();
        }
        return found;
    }

    private static Map<String, Object> described(final Throwable failure) {
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put(Worker.TYPE, failure.getClass().getSimpleName());
        values.put(Worker.MESSAGE, List.of(String.valueOf(failure.getMessage())));
        values.put(Worker.MODULE, failure.getClass().getPackageName());
        return values;
    }

    private static String text(final Throwable failure) {
        final StringWriter written = new StringWriter();
        try (PrintWriter printer = new PrintWriter(written)) {
            failure.printStackTrace(printer);
        }
        return written.toString();
    }
}
