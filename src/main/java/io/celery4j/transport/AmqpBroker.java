/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.transport;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageProperties;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

/**
 * A broker backed by a message queue service.
 *
 * <p>How a task is written for this transport is the business of
 * {@link AmqpMessages}: the headers travel as the delivery's headers and the
 * body as bytes.</p>
 *
 * <p>A message is taken unacknowledged and acknowledged when the worker says
 * it is done. What was taken but never acknowledged is returned to its queue
 * when a worker restores, and the service returns it anyway when the
 * connection is lost, which is what this transport promises and the other one
 * cannot.</p>
 *
 * <p>The channel is supplied rather than opened here. Closing this broker
 * closes it.</p>
 *
 * @since 0.2.0
 */
public final class AmqpBroker implements Broker {

    /**
     * How long a receive sleeps before asking the queues again.
     */
    private static final Duration TICK = Duration.ofMillis(50L);

    /**
     * Channel the messages travel on.
     */
    private final Channel channel;

    /**
     * Names a queue is known by.
     */
    private final Queues queues;

    /**
     * How a message is written for this transport and read back.
     */
    private final AmqpMessages messages;

    /**
     * Delivery tag of what has been taken but not acknowledged.
     */
    private final Map<Message, Long> holding;

    /**
     * Ctor.
     *
     * @param channel Channel the messages travel on
     */
    public AmqpBroker(final Channel channel) {
        this(channel, new Queues());
    }

    /**
     * Ctor.
     *
     * @param channel Channel the messages travel on
     * @param queues Names a queue is known by
     */
    public AmqpBroker(final Channel channel, final Queues queues) {
        this.channel = channel;
        this.queues = queues;
        this.messages = new AmqpMessages();
        this.holding = new ConcurrentHashMap<>();
    }

    @Override
    public void send(final Message message, final String queue) throws TransportException {
        final String name = this.queues.named(
            queue, message.properties().number(MessageProperties.PRIORITY, 0L)
        );
        try {
            this.channel.queueDeclare(name, true, false, false, Map.of());
            this.channel.basicPublish(
                "", name, this.messages.properties(message), this.messages.body(message)
            );
        } catch (final IOException ex) {
            throw new TransportException(String.format("queue %s cannot be written", queue), ex);
        }
    }

    @Override
    public Optional<Message> receive(final List<String> names, final Duration timeout)
        throws TransportException {
        final long deadline = System.nanoTime() + timeout.toNanos();
        Optional<Message> found = this.taken(names);
        while (found.isEmpty() && System.nanoTime() < deadline) {
            AmqpBroker.pause(
                Duration.ofNanos(Math.min(AmqpBroker.TICK.toNanos(), deadline - System.nanoTime()))
            );
            found = this.taken(names);
        }
        return found;
    }

    @Override
    public void done(final Message message) throws TransportException {
        final Long tag = this.holding.remove(message);
        if (tag != null) {
            try {
                this.channel.basicAck(tag, false);
            } catch (final IOException ex) {
                throw new TransportException("a delivery cannot be acknowledged", ex);
            }
        }
    }

    @Override
    public long restore() throws TransportException {
        long returned = 0L;
        for (final Map.Entry<Message, Long> held : Map.copyOf(this.holding).entrySet()) {
            this.holding.remove(held.getKey());
            try {
                this.channel.basicNack(held.getValue(), false, true);
            } catch (final IOException ex) {
                throw new TransportException("a delivery cannot be returned", ex);
            }
            returned = returned + 1L;
        }
        return returned;
    }

    @Override
    public void close() throws TransportException {
        try {
            this.channel.close();
        } catch (final IOException ex) {
            throw new TransportException("broker cannot be closed", ex);
        } catch (final TimeoutException ex) {
            throw new TransportException("broker took too long to close", ex);
        }
    }

    private Optional<Message> taken(final List<String> names) {
        Optional<Message> found = Optional.empty();
        for (final String name : names) {
            final GetResponse got;
            try {
                this.channel.queueDeclare(name, true, false, false, Map.of());
                got = this.channel.basicGet(name, false);
            } catch (final IOException ex) {
                throw new TransportException(String.format("queue %s cannot be read", name), ex);
            }
            if (got != null) {
                final Message message = this.messages.message(
                    got.getProps(), got.getBody(), got.getEnvelope().getRoutingKey()
                );
                this.holding.put(message, got.getEnvelope().getDeliveryTag());
                found = Optional.of(message);
                break;
            }
        }
        return found;
    }

    private static void pause(final Duration wait) {
        if (!wait.isNegative() && !wait.isZero()) {
            try {
                Thread.sleep(wait.toMillis(), wait.toNanosPart() % 1_000_000);
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new TransportException("waiting for a message was interrupted", ex);
            }
        }
    }
}
