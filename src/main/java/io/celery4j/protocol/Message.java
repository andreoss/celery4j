/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2024
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

/**
 * A message as a broker carries it: delivery properties, task headers and the
 * encoded body.
 *
 * <p>The body is held in the form it travels in, which the property named by
 * {@link MessageProperties#WRAPPING} describes. Reading the parameters out of
 * it is the work of a body codec, so a worker can inspect the headers of a
 * message whose body it cannot read.</p>
 *
 * @param properties Delivery properties
 * @param headers Task headers
 * @param body Body in the form it travels in
 * @since 0.1.0
 */
public record Message(MessageProperties properties, MessageHeaders headers, String body) {

    /**
     * Identifier of the task this message carries.
     *
     * @return The task identifier
     * @throws ProtocolException If the headers do not name it
     */
    public String id() throws ProtocolException {
        return this.headers.id();
    }

    /**
     * Name of the task this message carries.
     *
     * @return The task name
     * @throws ProtocolException If the headers do not name it
     */
    public String task() throws ProtocolException {
        return this.headers.task();
    }
}
