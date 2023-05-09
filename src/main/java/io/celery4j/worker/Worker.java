/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.worker;

import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageHeaders;
import io.celery4j.protocol.MessageProperties;
import io.celery4j.protocol.Protocols;
import io.celery4j.protocol.Schedule;
import io.celery4j.protocol.Task;
import io.celery4j.result.Backend;
import io.celery4j.result.State;
import io.celery4j.result.TaskResult;
import io.celery4j.transport.Broker;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Clock;
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
     * Clock the results are stamped with and the schedules judged against.
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
        final Instant now = this.clock.instant();
        final Optional<TaskResult> published;
        if (schedule.expired(now)) {
            published = Optional.of(
                this.store(message, this.ended(this.protocols.task(message), State.REVOKED))
            );
        } else if (schedule.due(now)) {
            published = Optional.of(
                this.store(
                    message,
                    this.outcome(new Envelope(message, queue, this.protocols.task(message)))
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
            .handle((value, failure) -> this.result(envelope.task(), value, failure))
            .join();
        if (State.RETRY.equals(result.state().name())) {
            this.broker.send(Worker.again(envelope.message()), envelope.queue());
        }
        return result;
    }

    private CompletableFuture<Object> running(final Envelope envelope) {
        return CompletableFuture.supplyAsync(
            () -> this.registry.of(envelope.task().name()).run(envelope.task()),
            Runnable::run
        );
    }

    private TaskResult result(final Task task, final Object value, final Throwable failure) {
        final TaskResult result;
        if (failure == null) {
            result = this.succeeded(task, value);
        } else {
            result = this.raised(task, Worker.cause(failure));
        }
        return result;
    }

    private TaskResult raised(final Task task, final Throwable failure) {
        final String state;
        if (failure instanceof RetryException) {
            state = State.RETRY;
        } else {
            state = State.FAILURE;
        }
        return this.ended(task, state)
            .with(TaskResult.VALUE, Worker.described(failure))
            .with(TaskResult.TRACEBACK, Worker.text(failure));
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
                .format(this.clock.instant())
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

    private static Message again(final Message message) {
        if (message.headers().text(MessageHeaders.TASK).isEmpty()) {
            throw new WorkerException("a task can only be retried under protocol two");
        }
        return new Message(
            message.properties(),
            message.headers().with(
                MessageHeaders.RETRIES,
                (int) message.headers().number(MessageHeaders.RETRIES, 0L) + 1
            ),
            message.body()
        );
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
