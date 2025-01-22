/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

/**
 * Reads and writes the body of a task message, which carries the parameters a
 * task is called with.
 *
 * @since 0.1.0
 */
public interface TaskBodyCodec {

    /**
     * Write a body.
     *
     * @param body Body to write
     * @return The body in the form it travels in
     * @throws ProtocolException If the body cannot be written
     */
    String encode(TaskBody body) throws ProtocolException;

    /**
     * Read a body.
     *
     * @param body Body in the form it travelled in
     * @return The parameters that body carries
     * @throws ProtocolException If the text is no body of this protocol
     */
    TaskBody decode(String body) throws ProtocolException;
}
