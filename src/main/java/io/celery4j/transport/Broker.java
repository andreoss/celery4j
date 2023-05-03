/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.transport;

import io.celery4j.protocol.Message;
import java.time.Duration;
import java.util.Optional;

/**
 * The queue a producer writes to and a worker reads from.
 *
 * <p>A broker carries messages and knows nothing of tasks. A message sent to a
 * queue is received from that queue and from no other, and a receive that
 * finds nothing within its timeout reports that rather than waiting on.</p>
 *
 * @since 0.1.0
 */
public interface Broker extends AutoCloseable {

    /**
     * Send a message to a queue.
     *
     * @param message Message to send
     * @param queue Queue it is routed to
     * @throws TransportException If the broker cannot take it
     */
    void send(Message message, String queue) throws TransportException;

    /**
     * Receive a message from a queue, waiting no longer than a timeout.
     *
     * @param queue Queue to read from
     * @param timeout How long to wait for one
     * @return The message, empty when none arrived in time
     * @throws TransportException If the broker cannot be read
     */
    Optional<Message> receive(String queue, Duration timeout) throws TransportException;

    @Override
    void close() throws TransportException;
}
