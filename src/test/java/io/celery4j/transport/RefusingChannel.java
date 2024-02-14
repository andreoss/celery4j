/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2024
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.transport;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import io.celery4j.protocol.Message;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.concurrent.TimeoutException;

/**
 * A channel that hands out one delivery and refuses everything else, for
 * cases about what a broker says when the service stops answering.
 *
 * <p>The channel of the queue service is an interface of a hundred methods,
 * and a case here needs five of them. What the others do is of no interest,
 * so this is a proxy rather than a class pretending to be a channel.</p>
 *
 * @since 1.0.1
 */
public final class RefusingChannel {

    /**
     * Delivery this channel hands out once asked for one, or nothing when it
     * refuses even that.
     */
    private final Message delivery;

    /**
     * Whether closing runs out of time rather than failing outright.
     */
    private final boolean slow;

    /**
     * Ctor.
     *
     * @param delivery Delivery this channel hands out
     */
    public RefusingChannel(final Message delivery) {
        this(delivery, false);
    }

    /**
     * Ctor.
     *
     * @param delivery Delivery this channel hands out, or null to refuse
     * @param slow Whether closing runs out of time rather than failing
     */
    public RefusingChannel(final Message delivery, final boolean slow) {
        this.delivery = delivery;
        this.slow = slow;
    }

    /**
     * The channel itself.
     *
     * @return A channel that refuses everything but a get
     */
    public Channel channel() {
        return (Channel) Proxy.newProxyInstance(
            Thread.currentThread().getContextClassLoader(),
            new Class<?>[] {Channel.class},
            (proxy, method, args) -> this.answer(method.getName())
        );
    }

    private Object answer(final String name) throws IOException, TimeoutException {
        if ("close".equals(name) && this.slow) {
            throw new TimeoutException("the queue service took too long to let go");
        }
        final Object answer;
        if ("isOpen".equals(name)) {
            answer = true;
        } else if ("basicGet".equals(name) && this.delivery != null) {
            answer = this.response();
        } else {
            throw new IOException("the queue service is not answering");
        }
        return answer;
    }

    private GetResponse response() {
        final AmqpMessages messages = new AmqpMessages();
        return new GetResponse(
            new Envelope(1L, false, "", "live"),
            messages.properties(this.delivery),
            messages.body(this.delivery),
            0
        );
    }
}
