/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2024
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.transport;

import io.celery4j.protocol.Message;
import io.celery4j.protocol.ProtocolV2;
import io.celery4j.protocol.Task;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for the parts of {@link Reconnecting} that only a broker which
 * will not work reaches.
 *
 * @since 1.0.1
 */
final class ReconnectingFailureTest {

    /**
     * Queue the cases name.
     */
    private static final String QUEUE = "unreachable";

    @AfterEach
    void clear() {
        Thread.interrupted();
    }

    @Test
    void passesOnLettingGoOfAMessageNobodyHolds() {
        Assertions.assertDoesNotThrow(
            () -> ReconnectingFailureTest.broker().done(ReconnectingFailureTest.message())
        );
    }

    @Test
    void passesOnRestoring() {
        Assertions.assertThrows(
            TransportException.class, () -> ReconnectingFailureTest.broker().restore()
        );
    }

    @Test
    void passesOnClosing() {
        Assertions.assertThrows(
            TransportException.class, () -> ReconnectingFailureTest.broker().close()
        );
    }

    @Test
    void triesAsOftenAsItWasToldWithoutBeingTold() {
        Assertions.assertThrows(
            TransportException.class,
            () -> new Reconnecting(new RedisBroker(new FailingPool()))
                .receive(ReconnectingFailureTest.QUEUE, Duration.ofMillis(10L))
        );
    }

    @Test
    void stopsTryingWhenTheThreadIsInterrupted() {
        Thread.currentThread().interrupt();
        Assertions.assertThrows(
            TransportException.class,
            () -> new Reconnecting(
                new RedisBroker(new FailingPool()), 3, Duration.ofSeconds(5L)
            ).receive(ReconnectingFailureTest.QUEUE, Duration.ofMillis(10L))
        );
    }

    private static Broker broker() {
        return new Reconnecting(new RedisBroker(new FailingPool()), 2, Duration.ofMillis(1L));
    }

    private static Message message() {
        return new ProtocolV2().message(
            new Task("task-one", "proj.java.sum", List.of(2, 3), Map.of()),
            ReconnectingFailureTest.QUEUE
        );
    }
}
