/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.worker;

/**
 * Raised by a job that asks to be run again.
 *
 * <p>A retry is not a failure: the worker sends the message back to the queue
 * with its retry count raised, and records that the task is waiting to be run
 * again rather than that it ended.</p>
 *
 * @since 0.1.1
 */
public final class RetryException extends RuntimeException {

    /**
     * Serial version identifier.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Ctor.
     *
     * @param message Why the task asks to be run again
     */
    public RetryException(final String message) {
        this(message, null);
    }

    /**
     * Ctor.
     *
     * @param message Why the task asks to be run again
     * @param cause The failure that led to the retry
     */
    public RetryException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
