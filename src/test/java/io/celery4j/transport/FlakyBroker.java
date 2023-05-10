/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.transport;

import io.celery4j.protocol.Message;
import io.celery4j.protocol.ProtocolV2;
import io.celery4j.protocol.Task;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A broker that fails a given number of times before it works, for cases about
 * what happens when a broker is briefly unreachable.
 *
 * @since 0.2.0
 */
final class FlakyBroker implements Broker {

    /**
     * Broker underneath.
     */
    private final Broker origin;

    /**
     * How many failures are left to hand out.
     */
    private final AtomicInteger failures;

    /**
     * How many calls were made.
     */
    private final AtomicInteger calls;

    /**
     * Ctor.
     *
     * @param origin Broker underneath
     * @param failures How many failures to hand out before working
     * @param calls How many calls were made
     */
    FlakyBroker(final Broker origin, final AtomicInteger failures, final AtomicInteger calls) {
        this.origin = origin;
        this.failures = failures;
        this.calls = calls;
    }

    @Override
    public void send(final Message message, final String queue) {
        this.flake();
        this.origin.send(message, queue);
    }

    @Override
    public Optional<Message> receive(final List<String> queues, final Duration timeout) {
        this.flake();
        this.origin.send(FlakyBroker.parked(), queues.get(0));
        return this.origin.receive(queues, timeout);
    }

    @Override
    public void close() {
        this.origin.close();
    }

    private void flake() {
        this.calls.incrementAndGet();
        if (this.failures.getAndDecrement() > 0) {
            throw new TransportException("the broker is not reachable");
        }
    }

    private static Message parked() {
        return new ProtocolV2().message(
            new Task("task-flaky", "proj.tasks.add", List.of(2, 2), Map.of()), "important"
        );
    }
}
