/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.transport;

import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageCodec;
import io.celery4j.protocol.ProtocolException;

/**
 * A codec that refuses every message, for cases about what a broker says when
 * what it holds cannot be written or read.
 *
 * @since 1.0.1
 */
public final class RefusingCodec implements MessageCodec {

    /**
     * Ctor.
     */
    public RefusingCodec() {
        // this codec has nothing to hold
    }

    @Override
    public byte[] encode(final Message message) throws ProtocolException {
        throw new ProtocolException("this codec writes nothing");
    }

    @Override
    public Message decode(final byte[] raw) throws ProtocolException {
        throw new ProtocolException("this codec reads nothing");
    }
}
