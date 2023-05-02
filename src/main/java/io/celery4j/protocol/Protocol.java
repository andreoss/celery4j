/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

/**
 * A version of the task message protocol.
 *
 * <p>A version decides where the meta data of a task lives: version two puts
 * it in the headers, version one puts all of it in the body.</p>
 *
 * @since 0.1.0
 */
public interface Protocol {

    /**
     * Build the message that asks for a task to be run.
     *
     * @param task Task to run
     * @param queue Queue the task is routed to
     * @return The message a broker carries
     * @throws ProtocolException If the message cannot be written
     */
    Message message(Task task, String queue) throws ProtocolException;

    /**
     * Read the task a message asks for.
     *
     * @param message Message a broker delivered
     * @return The task that message asks for
     * @throws ProtocolException If the message is no task of this version
     */
    Task task(Message message) throws ProtocolException;
}
