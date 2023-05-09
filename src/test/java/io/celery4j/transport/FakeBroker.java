/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.transport;

import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageProperties;
import java.time.Duration;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * A broker that keeps its queues in memory, for cases that need a broker but
 * not a store.
 *
 * @since 0.1.0
 */
public final class FakeBroker implements Broker {

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
    public FakeBroker(final Map<String, Deque<Message>> queues, final Queues names) {
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
    public Optional<Message> receive(final List<String> from, final Duration timeout) {
        return from.stream()
            .map(this.queues::get)
            .filter(Objects::nonNull)
            .map(Deque::pollLast)
            .filter(Objects::nonNull)
            .findFirst();
    }

    @Override
    public void close() {
        this.queues.clear();
    }
}
