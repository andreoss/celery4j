/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.result;

/**
 * Thrown when a result cannot be stored, read, or waited for, and when the
 * task it belongs to failed.
 *
 * @since 0.1.0
 */
public final class ResultException extends RuntimeException {

    /**
     * Serial version identifier.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Ctor.
     *
     * @param message Description of what happened
     */
    public ResultException(final String message) {
        this(message, null);
    }

    /**
     * Ctor.
     *
     * @param message Description of what happened
     * @param cause The failure this one was raised from
     */
    public ResultException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
