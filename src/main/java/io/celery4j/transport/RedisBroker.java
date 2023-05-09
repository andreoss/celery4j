/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
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
import java.util.Optional;
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
 * <p>The pool is supplied rather than opened here, so that a caller decides
 * how many connections there are and how they are configured. Closing this
 * broker closes that pool.</p>
 *
 * @since 0.1.0
 */
public final class RedisBroker implements Broker {

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
     * Ctor.
     *
     * @param pool Pool the connections come from
     */
    public RedisBroker(final JedisPool pool) {
        this(pool, RedisBroker.CODEC, RedisBroker.NAMES);
    }

    /**
     * Ctor.
     *
     * @param pool Pool the connections come from
     * @param codec Codec the messages are read and written with
     * @param queues Names a queue is known by
     */
    public RedisBroker(final JedisPool pool, final MessageCodec codec, final Queues queues) {
        this.pool = pool;
        this.codec = codec;
        this.queues = queues;
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
        final List<byte[]> popped;
        try (Jedis jedis = this.pool.getResource()) {
            popped = jedis.brpop(RedisBroker.seconds(timeout), RedisBroker.keys(names));
        } catch (final JedisException ex) {
            throw new TransportException(String.format("queues %s cannot be read", names), ex);
        }
        final Optional<Message> message;
        if (popped == null || popped.size() < 2) {
            message = Optional.empty();
        } else {
            message = Optional.of(this.read(popped.get(1)));
        }
        return message;
    }

    @Override
    public void close() throws TransportException {
        try {
            this.pool.close();
        } catch (final JedisException ex) {
            throw new TransportException("broker cannot be closed", ex);
        }
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

    private static byte[] key(final String name) {
        return name.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[][] keys(final List<String> names) {
        return names.stream().map(RedisBroker::key).toArray(byte[][]::new);
    }

    private static double seconds(final Duration timeout) {
        return Math.max(0.001, timeout.toMillis() / (double) 1000L);
    }
}
