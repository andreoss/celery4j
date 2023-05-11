/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.transport;

import io.celery4j.protocol.Message;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A broker that tries again when the one underneath fails.
 *
 * <p>A broker that is briefly unreachable is the ordinary case, not an
 * exceptional one: a service restarts, a connection is dropped, a name takes a
 * moment to resolve. Each attempt waits longer than the last, and after the
 * allowed number the failure is handed on rather than hidden, so a caller
 * still learns that the broker is gone.</p>
 *
 * <p>A send that failed may already have been taken by the broker, so a retry
 * can deliver the same message twice. A consumer of this protocol is expected
 * to stand that, since the protocol promises delivery at least once, not
 * exactly once.</p>
 *
 * @since 0.2.0
 */
public final class Reconnecting implements Broker {

    /**
     * How long the first wait is when nothing else was said.
     */
    private static final Duration FIRST = Duration.ofMillis(100L);

    /**
     * Broker underneath.
     */
    private final Broker origin;

    /**
     * How many attempts are made.
     */
    private final int attempts;

    /**
     * How long the first wait is, doubling with every attempt after it.
     */
    private final Duration first;

    /**
     * Ctor.
     *
     * @param origin Broker underneath
     */
    public Reconnecting(final Broker origin) {
        this(origin, 3, Reconnecting.FIRST);
    }

    /**
     * Ctor.
     *
     * @param origin Broker underneath
     * @param attempts How many attempts are made
     * @param first How long the first wait is
     */
    public Reconnecting(final Broker origin, final int attempts, final Duration first) {
        this.origin = origin;
        this.attempts = attempts;
        this.first = first;
    }

    @Override
    public void send(final Message message, final String queue) throws TransportException {
        this.tried(
            () -> {
                this.origin.send(message, queue);
                return Optional.empty();
            }
        );
    }

    @Override
    public Optional<Message> receive(final List<String> queues, final Duration timeout)
        throws TransportException {
        return this.tried(() -> this.origin.receive(queues, timeout));
    }

    @Override
    public void done(final Message message) throws TransportException {
        this.origin.done(message);
    }

    @Override
    public long restore() throws TransportException {
        return this.origin.restore();
    }

    @Override
    public void close() throws TransportException {
        this.origin.close();
    }

    private Optional<Message> tried(final Supplier<Optional<Message>> step) {
        Optional<Message> done = Optional.empty();
        Optional<TransportException> last = Optional.empty();
        for (int attempt = 0; attempt < this.attempts; attempt = attempt + 1) {
            try {
                done = step.get();
                last = Optional.empty();
                break;
            } catch (final TransportException ex) {
                last = Optional.of(ex);
                if (attempt + 1 < this.attempts) {
                    Reconnecting.pause(this.first.multipliedBy(1L << attempt));
                }
            }
        }
        if (last.isPresent()) {
            throw last.orElseThrow();
        }
        return done;
    }

    private static void pause(final Duration wait) {
        try {
            Thread.sleep(wait.toMillis());
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new TransportException("waiting to try again was interrupted", ex);
        }
    }
}
