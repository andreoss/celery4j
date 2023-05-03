/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.transport;

import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageProperties;
import java.time.Duration;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * A broker that keeps its queues in memory, for cases that need a broker but
 * not a store.
 *
 * @since 0.1.0
 */
final class FakeBroker implements Broker {

    /**
     * Queues by name.
     */
    private final Map<String, Deque<Message>> queues;

    /**
     * Names a queue is known by.
     */
    private final Queues names;

    /**
     * Ctor.
     *
     * @param queues Queues by name
     * @param names Names a queue is known by
     */
    FakeBroker(final Map<String, Deque<Message>> queues, final Queues names) {
        this.queues = queues;
        this.names = names;
    }

    @Override
    public void send(final Message message, final String queue) {
        this.queues.computeIfAbsent(
            this.names.named(
                queue, message.properties().number(MessageProperties.PRIORITY, 0L)
            ),
            key -> new ConcurrentLinkedDeque<>()
        ).addFirst(message);
    }

    @Override
    public Optional<Message> receive(final String queue, final Duration timeout) {
        return Optional.ofNullable(this.queues.get(queue)).map(Deque::pollLast);
    }

    @Override
    public void close() {
        this.queues.clear();
    }
}
