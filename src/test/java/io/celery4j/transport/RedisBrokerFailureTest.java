/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.transport;

import io.celery4j.protocol.Message;
import io.celery4j.protocol.ProtocolV2;
import io.celery4j.protocol.Task;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for what {@link RedisBroker} says when the store will not talk to
 * it.
 *
 * <p>Every one of these paths is a line of this library turning a failure of
 * somebody else's into one of its own, and none of them are reached by a case
 * that runs against a store which works.</p>
 *
 * @since 1.0.1
 */
final class RedisBrokerFailureTest {

    /**
     * Queue the cases name.
     */
    private static final String QUEUE = "unreachable";

    @Test
    void refusesToSendWhenTheStoreWillNot() {
        Assertions.assertThrows(
            TransportException.class,
            () -> RedisBrokerFailureTest.broker()
                .send(RedisBrokerFailureTest.message(), RedisBrokerFailureTest.QUEUE)
        );
    }

    @Test
    void saysWhichQueueCouldNotBeWritten() {
        Assertions.assertTrue(
            Assertions.assertThrows(
                TransportException.class,
                () -> RedisBrokerFailureTest.broker()
                    .send(RedisBrokerFailureTest.message(), RedisBrokerFailureTest.QUEUE)
            ).getMessage().contains(RedisBrokerFailureTest.QUEUE)
        );
    }

    @Test
    void refusesToReceiveWhenTheStoreWillNot() {
        Assertions.assertThrows(
            TransportException.class,
            () -> RedisBrokerFailureTest.broker()
                .receive(RedisBrokerFailureTest.QUEUE, Duration.ofMillis(10L))
        );
    }

    @Test
    void refusesToRestoreWhenTheStoreWillNot() {
        Assertions.assertThrows(
            TransportException.class, () -> RedisBrokerFailureTest.broker().restore()
        );
    }

    @Test
    void refusesToCloseWhenTheStoreWillNot() {
        Assertions.assertThrows(
            TransportException.class, () -> RedisBrokerFailureTest.broker().close()
        );
    }

    private static Broker broker() {
        return new RedisBroker(new FailingPool());
    }

    private static Message message() {
        return new ProtocolV2().message(
            new Task("task-one", "proj.java.sum", List.of(2, 3), Map.of()),
            RedisBrokerFailureTest.QUEUE
        );
    }
}
