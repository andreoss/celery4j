/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.transport;

/**
 * Thrown when a broker cannot carry a message.
 *
 * @since 0.1.0
 */
public final class TransportException extends RuntimeException {

    /**
     * Serial version identifier.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Ctor.
     *
     * @param message Description of what failed
     */
    public TransportException(final String message) {
        this(message, null);
    }

    /**
     * Ctor.
     *
     * @param message Description of what failed
     * @param cause The failure this one was raised from
     */
    public TransportException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
