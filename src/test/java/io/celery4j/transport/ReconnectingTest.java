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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link Reconnecting}.
 *
 * @since 0.2.0
 */
final class ReconnectingTest {

    /**
     * Queue used across the cases.
     */
    private static final String QUEUE = "important";

    /**
     * How long the cases wait, which is short because nothing really waits.
     */
    private static final Duration WAIT = Duration.ofMillis(10L);

    @Test
    void sendsThroughToTheBrokerUnderneath() {
        final Broker under = ReconnectingTest.working();
        new Reconnecting(under, 3, ReconnectingTest.WAIT)
            .send(ReconnectingTest.message(), ReconnectingTest.QUEUE);
        Assertions.assertTrue(
            under.receive(ReconnectingTest.QUEUE, ReconnectingTest.WAIT).isPresent()
        );
    }

    @Test
    void receivesThroughToTheBrokerUnderneath() {
        final Broker under = ReconnectingTest.working();
        under.send(ReconnectingTest.message(), ReconnectingTest.QUEUE);
        Assertions.assertTrue(
            new Reconnecting(under, 3, ReconnectingTest.WAIT)
                .receive(ReconnectingTest.QUEUE, ReconnectingTest.WAIT)
                .isPresent()
        );
    }

    @Test
    void triesAgainAfterAFailedReceive() {
        Assertions.assertTrue(
            new Reconnecting(
                ReconnectingTest.failing(2), 3, ReconnectingTest.WAIT
            ).receive(ReconnectingTest.QUEUE, ReconnectingTest.WAIT).isPresent()
        );
    }

    @Test
    void triesAgainAfterAFailedSend() {
        final Broker under = ReconnectingTest.failing(1);
        new Reconnecting(under, 3, ReconnectingTest.WAIT)
            .send(ReconnectingTest.message(), ReconnectingTest.QUEUE);
        Assertions.assertTrue(
            under.receive(ReconnectingTest.QUEUE, ReconnectingTest.WAIT).isPresent()
        );
    }

    @Test
    void givesUpAfterTheAttemptsItWasAllowed() {
        final Broker broker = new Reconnecting(
            ReconnectingTest.failing(5), 2, ReconnectingTest.WAIT
        );
        Assertions.assertThrows(
            TransportException.class,
            () -> broker.receive(ReconnectingTest.QUEUE, ReconnectingTest.WAIT)
        );
    }

    @Test
    void countsEveryAttemptItWasAllowed() {
        final AtomicInteger tries = new AtomicInteger();
        final Broker broker = new Reconnecting(
            ReconnectingTest.counting(tries), 3, ReconnectingTest.WAIT
        );
        Assertions.assertThrows(
            TransportException.class,
            () -> broker.receive(ReconnectingTest.QUEUE, ReconnectingTest.WAIT)
        );
        Assertions.assertEquals(3, tries.get());
    }

    @Test
    void closesTheBrokerUnderneath() {
        final Broker under = ReconnectingTest.working();
        under.send(ReconnectingTest.message(), ReconnectingTest.QUEUE);
        new Reconnecting(under, 3, ReconnectingTest.WAIT).close();
        Assertions.assertEquals(
            Optional.empty(), under.receive(ReconnectingTest.QUEUE, ReconnectingTest.WAIT)
        );
    }

    @Test
    void reportsAnEmptyQueueWithoutTryingAgain() {
        Assertions.assertEquals(
            Optional.empty(),
            new Reconnecting(ReconnectingTest.working(), 3, ReconnectingTest.WAIT)
                .receive(ReconnectingTest.QUEUE, ReconnectingTest.WAIT)
        );
    }

    private static Broker working() {
        return new FakeBroker(new ConcurrentHashMap<>(), new Queues(), new ConcurrentHashMap<>());
    }

    private static Broker failing(final int times) {
        return new FlakyBroker(
            new FakeBroker(new ConcurrentHashMap<>(), new Queues(), new ConcurrentHashMap<>()),
            new AtomicInteger(times),
            new AtomicInteger()
        );
    }

    private static Broker counting(final AtomicInteger tries) {
        return new FlakyBroker(
            new FakeBroker(new ConcurrentHashMap<>(), new Queues(), new ConcurrentHashMap<>()),
            new AtomicInteger(Integer.MAX_VALUE),
            tries
        );
    }

    private static Message message() {
        return new ProtocolV2().message(
            new Task("task-one", "proj.tasks.add", List.of(2, 2), Map.of()),
            ReconnectingTest.QUEUE
        );
    }
}
