/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2024
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.worker;

import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageHeaders;
import io.celery4j.protocol.Protocols;
import io.celery4j.protocol.Schedule;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;

/**
 * What a worker decides for itself rather than reading from a message: which
 * protocol versions it understands, what clock it judges time by, how it
 * retries, and how long a task may run when the message names no limit.
 *
 * @since 0.2.0
 */
public final class Options {

    /**
     * Protocol versions used when none were given.
     */
    private static final Protocols VERSIONS = new Protocols();

    /**
     * Retry policy used when none was given.
     */
    private static final RetryPolicy RETRIES = new RetryPolicy();

    /**
     * Protocol versions a worker understands.
     */
    private final Protocols protocols;

    /**
     * Clock a worker judges time by.
     */
    private final Clock clock;

    /**
     * How a worker retries.
     */
    private final RetryPolicy retries;

    /**
     * How long a task may run when its message names no limit, null for as
     * long as it takes.
     */
    private final Duration limit;

    /**
     * Ctor.
     */
    public Options() {
        this(Options.VERSIONS, Clock.systemUTC(), Options.RETRIES, null);
    }

    /**
     * Ctor.
     *
     * @param protocols Protocol versions a worker understands
     * @param clock Clock a worker judges time by
     * @param retries How a worker retries
     * @param limit How long a task may run when its message names no limit,
     *  null for as long as it takes
     */
    public Options(
        final Protocols protocols,
        final Clock clock,
        final RetryPolicy retries,
        final Duration limit
    ) {
        this.protocols = protocols;
        this.clock = clock;
        this.retries = retries;
        this.limit = limit;
    }

    /**
     * Protocol versions a worker understands.
     *
     * @return The versions
     */
    public Protocols protocols() {
        return this.protocols;
    }

    /**
     * Clock a worker judges time by.
     *
     * @return The clock
     */
    public Clock clock() {
        return this.clock;
    }

    /**
     * How a worker retries.
     *
     * @return The policy
     */
    public RetryPolicy retries() {
        return this.retries;
    }

    /**
     * How long a task may run, which its message may say and these options
     * answer for when it does not.
     *
     * @param message Message asking for the task
     * @return The limit, empty when a task may take as long as it takes
     */
    public Optional<Duration> limit(final Message message) {
        return new Schedule(message.headers()).limit().or(() -> Optional.ofNullable(this.limit));
    }

    /**
     * Whether a task that asked to be retried may be.
     *
     * @param message Message asking for the task
     * @return True when another attempt is allowed
     */
    public boolean retriable(final Message message) {
        return this.retries.allows(message.headers().number(MessageHeaders.RETRIES, 0L));
    }
}
