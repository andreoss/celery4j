/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.transport;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

/**
 * A pool that refuses everything, for cases about what a broker says when the
 * store is not there.
 *
 * <p>A store that is merely unreachable would do for some of this, and not for
 * closing, which succeeds against an address nobody answers at. A pool that
 * refuses on purpose covers both without waiting for a timeout.</p>
 *
 * @since 1.0.1
 */
public final class FailingPool extends JedisPool {

    /**
     * Ctor.
     */
    public FailingPool() {
        super();
    }

    @Override
    public Jedis getResource() {
        throw new JedisException("the store refuses to hand out a connection");
    }

    @Override
    public void close() {
        throw new JedisException("the store refuses to be let go");
    }
}
