/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.transport;

/**
 * Where a message is published, and who declares the queue it lands in.
 *
 * <p>A queue service routes by exchange and routing key. The one exchange
 * every service has is the nameless one, which delivers to the queue named by
 * the routing key and needs nothing declared beforehand; that is what a broker
 * uses when nobody says otherwise, and it declares the queue itself so that a
 * message written before anyone listens is not thrown away.</p>
 *
 * <p>A deployment that routes through an exchange of its own declares that
 * exchange, the queues and the bindings between them, usually with arguments a
 * library knows nothing about. A broker that declared a queue there too would
 * be refused by the service for disagreeing with whoever declared it first,
 * which is why a named exchange leaves declaring alone.</p>
 *
 * @since 1.0.0
 */
public final class Exchange {

    /**
     * The nameless exchange, which routes by queue name.
     */
    public static final String NONE = "";

    /**
     * Name of the exchange.
     */
    private final String label;

    /**
     * Whether the broker declares the queues it touches.
     */
    private final boolean declaring;

    /**
     * Ctor.
     */
    public Exchange() {
        this(Exchange.NONE, true);
    }

    /**
     * Ctor.
     *
     * @param label Name of the exchange, whose topology is declared elsewhere
     */
    public Exchange(final String label) {
        this(label, false);
    }

    /**
     * Ctor.
     *
     * @param label Name of the exchange
     * @param declaring Whether the broker declares the queues it touches
     */
    public Exchange(final String label, final boolean declaring) {
        this.label = label;
        this.declaring = declaring;
    }

    /**
     * Name of the exchange.
     *
     * @return The name, empty for the nameless one
     */
    public String name() {
        return this.label;
    }

    /**
     * Whether the broker declares the queues it touches.
     *
     * @return True when it declares them
     */
    public boolean declares() {
        return this.declaring;
    }
}
