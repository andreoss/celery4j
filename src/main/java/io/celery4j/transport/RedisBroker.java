/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2024
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.transport;

import io.celery4j.protocol.JsonMessageCodec;
import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageCodec;
import io.celery4j.protocol.MessageProperties;
import io.celery4j.protocol.ProtocolException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

/**
 * A broker backed by a key value store that carries each queue as a list.
 *
 * <p>A message is pushed at the head of the list and read from its tail, so
 * that messages leave in the order they arrived. The queue a message goes to
 * is the one its priority names, which is how this transport carries
 * priorities it has none of its own.</p>
 *
 * <p>A received message is moved, in one step, to a list of what this worker
 * is holding. It leaves that list when the worker says it is done with it, and
 * whatever is left there is returned to the queue it names when a worker
 * restores. A worker that dies therefore loses no message, at the price of a
 * message possibly arriving twice, which is what this protocol promises
 * anyway.</p>
 *
 * <p>The list of held messages is this library's own bookkeeping and no part
 * of the protocol. Another consumer of the same queue neither sees nor needs
 * it.</p>
 *
 * <p>The pool is supplied rather than opened here, so that a caller decides
 * how many connections there are and how they are configured. Closing this
 * broker closes that pool.</p>
 *
 * @since 0.1.0
 */
public final class RedisBroker implements Broker {

    /**
     * Name of the list holding what a worker has taken, when none was given.
     */
    public static final String HELD = "held-messages";

    /**
     * How long a receive sleeps before asking the queues again.
     */
    private static final Duration TICK = Duration.ofMillis(50L);

    /**
     * Codec used when none was given.
     */
    private static final MessageCodec CODEC = new JsonMessageCodec();

    /**
     * Queue naming used when none was given.
     */
    private static final Queues NAMES = new Queues();

    /**
     * Pool the connections come from.
     */
    private final JedisPool pool;

    /**
     * Codec the messages are read and written with.
     */
    private final MessageCodec codec;

    /**
     * Names a queue is known by.
     */
    private final Queues queues;

    /**
     * Name of the list holding what this worker has taken.
     */
    private final String held;

    /**
     * What this worker is holding, as it arrived.
     */
    private final Map<Message, byte[]> holding;

    /**
     * Ctor.
     *
     * @param pool Pool the connections come from
     */
    public RedisBroker(final JedisPool pool) {
        this(pool, RedisBroker.CODEC, RedisBroker.NAMES, RedisBroker.HELD);
    }

    /**
     * Ctor.
     *
     * @param pool Pool the connections come from
     * @param codec Codec the messages are read and written with
     * @param queues Names a queue is known by
     * @param held Name of the list holding what this worker has taken
     */
    public RedisBroker(
        final JedisPool pool, final MessageCodec codec, final Queues queues, final String held
    ) {
        this.pool = pool;
        this.codec = codec;
        this.queues = queues;
        this.held = held;
        this.holding = new ConcurrentHashMap<>();
    }

    @Override
    public void send(final Message message, final String queue) throws TransportException {
        try (Jedis jedis = this.pool.getResource()) {
            jedis.lpush(
                RedisBroker.key(
                    this.queues.named(
                        queue, message.properties().number(MessageProperties.PRIORITY, 0L)
                    )
                ),
                this.written(message)
            );
        } catch (final JedisException ex) {
            throw new TransportException(String.format("queue %s cannot be written", queue), ex);
        }
    }

    @Override
    public Optional<Message> receive(final List<String> names, final Duration timeout)
        throws TransportException {
        final long deadline = System.nanoTime() + timeout.toNanos();
        Optional<Message> found = this.taken(names);
        while (found.isEmpty() && System.nanoTime() < deadline) {
            RedisBroker.pause(
                Duration.ofNanos(
                    Math.min(RedisBroker.TICK.toNanos(), deadline - System.nanoTime())
                )
            );
            found = this.taken(names);
        }
        return found;
    }

    @Override
    public void done(final Message message) throws TransportException {
        final byte[] raw = this.holding.remove(message);
        if (raw != null) {
            try (Jedis jedis = this.pool.getResource()) {
                jedis.lrem(RedisBroker.key(this.held), 1L, raw);
            } catch (final JedisException ex) {
                throw new TransportException("a held message cannot be released", ex);
            }
        }
    }

    @Override
    public long restore() throws TransportException {
        long returned = 0L;
        try (Jedis jedis = this.pool.getResource()) {
            byte[] raw = jedis.rpop(RedisBroker.key(this.held));
            while (raw != null) {
                final Message message = this.read(raw);
                this.holding.remove(message);
                jedis.lpush(RedisBroker.key(RedisBroker.routing(message)), raw);
                returned = returned + 1L;
                raw = jedis.rpop(RedisBroker.key(this.held));
            }
        } catch (final JedisException ex) {
            throw new TransportException("held messages cannot be returned", ex);
        }
        return returned;
    }

    @Override
    public void close() throws TransportException {
        try {
            this.pool.close();
        } catch (final JedisException ex) {
            throw new TransportException("broker cannot be closed", ex);
        }
    }

    private Optional<Message> taken(final List<String> names) {
        Optional<Message> found = Optional.empty();
        try (Jedis jedis = this.pool.getResource()) {
            for (final String name : names) {
                final byte[] raw = jedis.rpoplpush(
                    RedisBroker.key(name), RedisBroker.key(this.held)
                );
                if (raw != null) {
                    final Message message = this.read(raw);
                    this.holding.put(message, raw);
                    found = Optional.of(message);
                    break;
                }
            }
        } catch (final JedisException ex) {
            throw new TransportException(String.format("queues %s cannot be read", names), ex);
        }
        return found;
    }

    private byte[] written(final Message message) {
        try {
            return this.codec.encode(message);
        } catch (final ProtocolException ex) {
            throw new TransportException("message cannot be written", ex);
        }
    }

    private Message read(final byte[] raw) {
        try {
            return this.codec.decode(raw);
        } catch (final ProtocolException ex) {
            throw new TransportException("message cannot be read", ex);
        }
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

    private static byte[] key(final String name) {
        return name.getBytes(StandardCharsets.UTF_8);
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
