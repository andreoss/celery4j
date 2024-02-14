/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2024
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

/**
 * Thrown when a message cannot be read or written as the protocol prescribes.
 *
 * @since 0.1.0
 */
public final class ProtocolException extends RuntimeException {

    /**
     * Serial version identifier.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Ctor.
     *
     * @param message Description of what was rejected and why
     */
    public ProtocolException(final String message) {
        this(message, null);
    }

    /**
     * Ctor.
     *
     * @param message Description of what was rejected and why
     * @param cause The failure this one was raised from
     */
    public ProtocolException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
