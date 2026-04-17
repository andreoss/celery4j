/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
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
 * <p>It holds what it hands out in the same way the real adapter does, so the
 * cases about a worker dying mean the same thing here.</p>
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
     * What has been handed out but not seen through.
     */
    private final Map<Message, Message> holding;

    /**
     * Ctor.
     *
     * @param queues Queues by name
     * @param names Names a queue is known by
     * @param holding What has been handed out but not seen through
     */
    public FakeBroker(
        final Map<String, Deque<Message>> queues,
        final Queues names,
        final Map<Message, Message> holding
    ) {
        this.queues = queues;
        this.names = names;
        this.holding = holding;
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
            .findFirst()
            .map(this::hold);
    }

    @Override
    public void done(final Message message) {
        this.holding.remove(message);
    }

    @Override
    public long restore() {
        long returned = 0L;
        for (final Message message : List.copyOf(this.holding.values())) {
            this.holding.remove(message);
            this.send(message, FakeBroker.routing(message));
            returned = returned + 1L;
        }
        return returned;
    }

    @Override
    public void close() {
        this.queues.clear();
        this.holding.clear();
    }

    private Message hold(final Message message) {
        this.holding.put(message, message);
        return message;
    }

    private static String routing(final Message message) {
        String queue = "celery";
        final Object delivery = message.properties().asMap().get(MessageProperties.DELIVERY);
        if (delivery instanceof Map<?, ?> info
            && info.get(MessageProperties.ROUTING) instanceof String named) {
            queue = named;
        }
        return queue;
    }
}
