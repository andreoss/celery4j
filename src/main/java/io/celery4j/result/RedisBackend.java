/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.result;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

/**
 * A result store backed by a key value store, holding each result under the
 * key its task identifier names.
 *
 * <p>A result is kept for a lifetime and then forgotten, which is what keeps
 * a store from growing without bound. The pool is supplied rather than opened
 * here, and closing this store closes it.</p>
 *
 * @since 0.1.0
 */
public final class RedisBackend implements Backend {

    /**
     * What every result key starts with.
     */
    public static final String PREFIX = "celery-task-meta-";

    /**
     * How long a result is kept when no lifetime was given.
     */
    private static final Duration LIFETIME = Duration.ofDays(1L);

    /**
     * Codec used when none was given.
     */
    private static final JsonResults CODEC = new JsonResults();

    /**
     * Pool the connections come from.
     */
    private final JedisPool pool;

    /**
     * Codec the results are read and written with.
     */
    private final JsonResults codec;

    /**
     * How long a result is kept.
     */
    private final Duration lifetime;

    /**
     * Ctor.
     *
     * @param pool Pool the connections come from
     */
    public RedisBackend(final JedisPool pool) {
        this(pool, RedisBackend.CODEC, RedisBackend.LIFETIME);
    }

    /**
     * Ctor.
     *
     * @param pool Pool the connections come from
     * @param codec Codec the results are read and written with
     * @param lifetime How long a result is kept
     */
    public RedisBackend(
        final JedisPool pool, final JsonResults codec, final Duration lifetime
    ) {
        this.pool = pool;
        this.codec = codec;
        this.lifetime = lifetime;
    }

    @Override
    public void store(final TaskResult result) throws ResultException {
        try (Jedis jedis = this.pool.getResource()) {
            jedis.setex(
                RedisBackend.key(result.id()),
                this.lifetime.toSeconds(),
                this.codec.encode(result)
            );
        } catch (final JedisException ex) {
            throw new ResultException("result cannot be stored", ex);
        }
    }

    @Override
    public Optional<TaskResult> of(final String id) throws ResultException {
        final byte[] raw;
        try (Jedis jedis = this.pool.getResource()) {
            raw = jedis.get(RedisBackend.key(id));
        } catch (final JedisException ex) {
            throw new ResultException(String.format("result of %s cannot be read", id), ex);
        }
        return Optional.ofNullable(raw).map(this.codec::decode);
    }

    @Override
    public void close() throws ResultException {
        try {
            this.pool.close();
        } catch (final JedisException ex) {
            throw new ResultException("store cannot be closed", ex);
        }
    }

    private static byte[] key(final String id) {
        return String.format("%s%s", RedisBackend.PREFIX, id)
            .getBytes(StandardCharsets.UTF_8);
    }
}
