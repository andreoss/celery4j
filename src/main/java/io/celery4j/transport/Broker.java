/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.transport;

import io.celery4j.protocol.Message;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * The queue a producer writes to and a worker reads from.
 *
 * <p>A broker carries messages and knows nothing of tasks. A message sent to a
 * queue is received from that queue and from no other, and a receive that
 * finds nothing within its timeout reports that rather than waiting on.</p>
 *
 * <p>A receive may name several queues. They are served in the order given, so
 * a caller that names the queues of a priority highest first is served in that
 * order.</p>
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
     * Receive a message from the first of these queues that has one, waiting
     * no longer than a timeout.
     *
     * @param queues Queues to read from, in the order they are served
     * @param timeout How long to wait for one
     * @return The message, empty when none arrived in time
     * @throws TransportException If the broker cannot be read
     */
    Optional<Message> receive(List<String> queues, Duration timeout) throws TransportException;

    /**
     * Receive a message from a queue, waiting no longer than a timeout.
     *
     * @param queue Queue to read from
     * @param timeout How long to wait for one
     * @return The message, empty when none arrived in time
     * @throws TransportException If the broker cannot be read
     */
    default Optional<Message> receive(final String queue, final Duration timeout)
        throws TransportException {
        return this.receive(List.of(queue), timeout);
    }

    /**
     * Say that a message was seen through, so it is not returned to its queue
     * later.
     *
     * <p>An adapter that does not hold what it hands out has nothing to do
     * here, and says so by doing nothing.</p>
     *
     * @param message Message that was seen through
     * @throws TransportException If the broker cannot be told
     */
    void done(Message message) throws TransportException;

    /**
     * Return everything taken but never seen through to the queue it came
     * from.
     *
     * <p>This is what a worker calls when it starts, so that what an earlier
     * worker was holding when it died is run rather than lost.</p>
     *
     * @return How many messages were returned
     * @throws TransportException If the broker cannot be read or written
     */
    long restore() throws TransportException;

    @Override
    void close() throws TransportException;
}
