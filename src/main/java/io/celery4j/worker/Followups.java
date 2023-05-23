/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.worker;

import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageHeaders;
import io.celery4j.protocol.ProtocolException;
import io.celery4j.protocol.ProtocolV2;
import io.celery4j.protocol.Schedule;
import io.celery4j.protocol.Signature;
import io.celery4j.protocol.Task;
import io.celery4j.protocol.TaskBody;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The work that follows a task that finished.
 *
 * <p>A message says what comes after it in its own body. The rest of a chain
 * is written there last-first, so the call that runs next is the one at the
 * end of the list, and what is left travels on with it. A task written in the
 * older version says nothing about any of this, and nothing follows it.</p>
 *
 * @since 1.0.0
 */
final class Followups {

    /**
     * Version the follow-up is written in.
     */
    private final ProtocolV2 protocol;

    /**
     * Ctor.
     */
    Followups() {
        this(new ProtocolV2());
    }

    /**
     * Ctor.
     *
     * @param protocol Version the follow-up is written in
     */
    Followups(final ProtocolV2 protocol) {
        this.protocol = protocol;
    }

    /**
     * What is to be sent once a task has finished.
     *
     * @param envelope Message that finished, and where it came from
     * @param value What the task handed over
     * @return The messages that follow, empty when nothing does
     * @throws ProtocolException If what the body names is no call
     */
    List<Envelope> after(final Envelope envelope, final Object value) throws ProtocolException {
        final List<Envelope> next = new ArrayList<>(1);
        if (envelope.message().headers().text(MessageHeaders.TASK).isPresent()) {
            final TaskBody body = this.protocol.body(envelope.message());
            final List<Object> chain = new ArrayList<>(
                Followups.listed(body.embedded(TaskBody.CHAIN).orElse(null))
            );
            if (!chain.isEmpty()) {
                next.add(this.link(envelope, chain, value));
            }
            next.addAll(this.called(envelope, body, TaskBody.CALLBACKS, value));
        }
        return List.copyOf(next);
    }

    /**
     * What is to be sent once a task has failed.
     *
     * <p>What a message names for failure is called with the identity of the
     * task that failed, which is what the framework hands it and all it can
     * ask the store about.</p>
     *
     * @param envelope Message that failed, and where it came from
     * @return The messages that follow, empty when nothing does
     * @throws ProtocolException If what the body names is no call
     */
    List<Envelope> failed(final Envelope envelope) throws ProtocolException {
        final List<Envelope> next;
        if (envelope.message().headers().text(MessageHeaders.TASK).isPresent()) {
            next = this.called(
                envelope,
                this.protocol.body(envelope.message()),
                TaskBody.ERRBACKS,
                envelope.task().id()
            );
        } else {
            next = List.of();
        }
        return next;
    }

    /**
     * The same message again, for one more attempt.
     *
     * <p>Only a message of the newer version can say how many attempts it has
     * had, so only such a message can be sent back for another one.</p>
     *
     * @param envelope Message that asked for another attempt
     * @param options What the worker decides for itself
     * @return The message to send back
     * @throws WorkerException If the message cannot count its attempts
     */
    static Envelope retry(final Envelope envelope, final Options options) throws WorkerException {
        final Message message = envelope.message();
        if (message.headers().text(MessageHeaders.TASK).isEmpty()) {
            throw new WorkerException("a task can only be retried under protocol two");
        }
        final long retries = message.headers().number(MessageHeaders.RETRIES, 0L);
        return new Envelope(
            new Message(
                message.properties(),
                new Schedule(
                    message.headers().with(MessageHeaders.RETRIES, (int) retries + 1)
                ).at(options.clock().instant().plus(options.retries().delay(retries))),
                message.body()
            ),
            envelope.queue(),
            envelope.task()
        );
    }

    private List<Envelope> called(
        final Envelope envelope, final TaskBody body, final String field, final Object before
    ) throws ProtocolException {
        final List<Envelope> next = new ArrayList<>(1);
        for (final Object call : Followups.listed(body.embedded(field).orElse(null))) {
            next.add(this.sent(envelope, new Signature(Followups.fields(call)), before));
        }
        return List.copyOf(next);
    }

    private Envelope sent(
        final Envelope envelope, final Signature signature, final Object before
    ) throws ProtocolException {
        final Task task = signature.task(before);
        final String queue = signature.queue().orElse(envelope.queue());
        return Followups.addressed(
            envelope, this.protocol.message(task, task.body(), queue), queue, task
        );
    }

    private static Envelope addressed(
        final Envelope envelope, final Message written, final String queue, final Task task
    ) {
        return new Envelope(
            new Message(
                written.properties(),
                written.headers()
                    .with(MessageHeaders.PARENT, envelope.task().id())
                    .with(MessageHeaders.ROOT, Followups.root(envelope)),
                written.body()
            ),
            queue,
            task
        );
    }

    private Envelope link(final Envelope envelope, final List<Object> chain, final Object value)
        throws ProtocolException {
        final Signature signature = new Signature(
            Followups.fields(chain.remove(chain.size() - 1))
        );
        final Task task = signature.task(value);
        final String queue = signature.queue().orElse(envelope.queue());
        return Followups.addressed(
            envelope,
            this.protocol.message(
                task,
                new TaskBody(task.args(), task.kwargs(), Map.of(TaskBody.CHAIN, chain)),
                queue
            ),
            queue,
            task
        );
    }

    private static String root(final Envelope envelope) {
        return envelope.message()
            .headers()
            .text(MessageHeaders.ROOT)
            .orElseGet(() -> envelope.task().id());
    }

    private static Map<String, Object> fields(final Object value) throws ProtocolException {
        if (!(value instanceof Map<?, ?>)) {
            throw new ProtocolException(
                String.format("a chain link is no signature: %s", value)
            );
        }
        return Followups.copied((Map<?, ?>) value);
    }

    private static Map<String, Object> copied(final Map<?, ?> fields) {
        final Map<String, Object> copy = new LinkedHashMap<>();
        for (final Map.Entry<?, ?> entry : fields.entrySet()) {
            copy.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        return copy;
    }

    private static List<Object> listed(final Object value) {
        final List<Object> found;
        if (value instanceof List<?> items) {
            found = new ArrayList<>(items);
        } else {
            found = List.of();
        }
        return found;
    }
}
