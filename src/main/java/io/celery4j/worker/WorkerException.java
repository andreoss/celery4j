/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2024
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.worker;

/**
 * Thrown when a worker cannot run what it was asked to run.
 *
 * @since 0.1.0
 */
public final class WorkerException extends RuntimeException {

    /**
     * Serial version identifier.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Ctor.
     *
     * @param message Description of what went wrong
     */
    public WorkerException(final String message) {
        this(message, null);
    }

    /**
     * Ctor.
     *
     * @param message Description of what went wrong
     * @param cause The failure this one was raised from
     */
    public WorkerException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
