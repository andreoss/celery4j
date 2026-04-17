/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

/**
 * Reads and writes the form a message travels in over a transport that carries
 * text.
 *
 * @since 0.1.0
 */
public interface MessageCodec {

    /**
     * Write a message.
     *
     * @param message Message to write
     * @return The bytes a transport carries
     * @throws ProtocolException If the message cannot be written
     */
    byte[] encode(Message message) throws ProtocolException;

    /**
     * Read a message.
     *
     * @param raw Bytes a transport delivered
     * @return The message those bytes carry
     * @throws ProtocolException If the bytes are no message of this protocol
     */
    Message decode(byte[] raw) throws ProtocolException;
}
