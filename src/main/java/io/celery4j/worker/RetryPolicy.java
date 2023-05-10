/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.worker;

import java.time.Duration;

/**
 * How often a task may be retried and how long it waits in between.
 *
 * <p>The wait doubles with every attempt and stops growing at the longest one
 * allowed, so a task that keeps failing backs off instead of hammering. A task
 * that has used up its attempts is not retried again: it ends as a failure,
 * which is the outcome a caller can act on.</p>
 *
 * @since 0.2.0
 */
public final class RetryPolicy {

    /**
     * How long the first wait is when nothing else was said.
     */
    private static final Duration FIRST = Duration.ofSeconds(1L);

    /**
     * How long the longest wait is when nothing else was said.
     */
    private static final Duration LONGEST = Duration.ofMinutes(10L);

    /**
     * How many retries are allowed.
     */
    private final int attempts;

    /**
     * How long the first wait is.
     */
    private final Duration first;

    /**
     * How long the longest wait is.
     */
    private final Duration longest;

    /**
     * Ctor.
     */
    public RetryPolicy() {
        this(3, RetryPolicy.FIRST, RetryPolicy.LONGEST);
    }

    /**
     * Ctor.
     *
     * @param attempts How many retries are allowed
     * @param first How long the first wait is
     * @param longest How long the longest wait is
     */
    public RetryPolicy(final int attempts, final Duration first, final Duration longest) {
        this.attempts = attempts;
        this.first = first;
        this.longest = longest;
    }

    /**
     * Whether a task that has been retried this often may be retried again.
     *
     * @param retries How often it has been retried already
     * @return True when another attempt is allowed
     */
    public boolean allows(final long retries) {
        return retries < this.attempts;
    }

    /**
     * How long a task waits before its next attempt.
     *
     * @param retries How often it has been retried already
     * @return The wait, never longer than the longest allowed
     */
    public Duration delay(final long retries) {
        final Duration wait;
        if (retries >= Long.SIZE) {
            wait = this.longest;
        } else {
            wait = this.first.multipliedBy(1L << Math.max(0L, retries));
        }
        final Duration bounded;
        if (wait.compareTo(this.longest) > 0) {
            bounded = this.longest;
        } else {
            bounded = wait;
        }
        return bounded;
    }
}
