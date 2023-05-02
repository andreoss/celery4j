/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

/**
 * The protocol versions a consumer understands.
 *
 * <p>The version of a message is detected by the presence of a task name in
 * its headers, which is how version two is told from version one.</p>
 *
 * @since 0.1.0
 */
public final class Protocols {

    /**
     * Version used when the headers name a task.
     */
    private static final Protocol NEWER = new ProtocolV2();

    /**
     * Version used when they do not.
     */
    private static final Protocol OLDER = new ProtocolV1();

    /**
     * Version used when the headers name a task.
     */
    private final Protocol newer;

    /**
     * Version used when they do not.
     */
    private final Protocol older;

    /**
     * Ctor.
     */
    public Protocols() {
        this(Protocols.NEWER, Protocols.OLDER);
    }

    /**
     * Ctor.
     *
     * @param newer Version used when the headers name a task
     * @param older Version used when they do not
     */
    public Protocols(final Protocol newer, final Protocol older) {
        this.newer = newer;
        this.older = older;
    }

    /**
     * The version a message is written in.
     *
     * @param message Message a broker delivered
     * @return The protocol that reads it
     * @throws ProtocolException If the headers cannot be read
     */
    public Protocol of(final Message message) throws ProtocolException {
        final Protocol protocol;
        if (message.headers().text(MessageHeaders.TASK).isPresent()) {
            protocol = this.newer;
        } else {
            protocol = this.older;
        }
        return protocol;
    }

    /**
     * The task a message asks for, whichever version wrote it.
     *
     * @param message Message a broker delivered
     * @return The task that message asks for
     * @throws ProtocolException If the message is no task of either version
     */
    public Task task(final Message message) throws ProtocolException {
        return this.of(message).task(message);
    }
}
