/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.worker;

import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageHeaders;
import io.celery4j.protocol.MessageProperties;
import java.util.List;
import java.util.Map;

/**
 * What a message says about itself and about the delivery that carried it.
 *
 * <p>A worker reads two things from a message that are neither task nor
 * schedule: whether anybody wants its result, and which of the queues it was
 * listening to the message came from. Both are written by the producer and
 * both are optional, so both have an answer when nothing was said.</p>
 *
 * @since 1.0.0
 */
final class Requests {

    /**
     * Message this reads.
     */
    private final Message message;

    /**
     * Ctor.
     *
     * @param message Message this reads
     */
    Requests(final Message message) {
        this.message = message;
    }

    /**
     * Whether the producer said it wants no result.
     *
     * @return True when the result is to be thrown away
     */
    boolean ignored() {
        return this.message.headers().asMap().get(MessageHeaders.IGNORE) instanceof Boolean ignore
            && ignore;
    }

    /**
     * Queue the message came from.
     *
     * @param queues Queues the worker was listening to, the first one standing
     *  in when the delivery says nothing
     * @return The queue name
     */
    String queue(final List<String> queues) {
        final Object delivery = this.message.properties().asMap().get(MessageProperties.DELIVERY);
        final String name;
        if (delivery instanceof Map<?, ?> info
            && info.get(MessageProperties.ROUTING) instanceof String routed) {
            name = routed;
        } else {
            name = queues.get(0);
        }
        return name;
    }
}
