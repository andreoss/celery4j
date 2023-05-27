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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for what {@link AmqpBroker} says when the service stops
 * answering.
 *
 * <p>A service that refuses in the middle of a conversation is the one thing
 * no case against a working one can show, and it is where every line that
 * turns a failure of the service into a failure of this library lives.</p>
 *
 * @since 1.0.1
 */
final class AmqpBrokerFailureTest {

    /**
     * Queue the cases name.
     */
    private static final String QUEUE = "live";

    @Test
    void refusesToSendWhenTheServiceWillNotAnswer() {
        Assertions.assertThrows(
            TransportException.class,
            () -> AmqpBrokerFailureTest.broker()
                .send(AmqpBrokerFailureTest.message(), AmqpBrokerFailureTest.QUEUE)
        );
    }

    @Test
    void refusesToLetGoOfADeliveryWhenTheServiceWillNotAnswer() {
        final Broker broker = AmqpBrokerFailureTest.broker();
        final Message taken = broker
            .receive(AmqpBrokerFailureTest.QUEUE, Duration.ofMillis(100L))
            .orElseThrow();
        Assertions.assertThrows(TransportException.class, () -> broker.done(taken));
    }

    @Test
    void refusesToCloseWhenTheServiceWillNotAnswer() {
        Assertions.assertThrows(
            TransportException.class, AmqpBrokerFailureTest.broker()::close
        );
    }

    @Test
    void refusesToReturnADeliveryWhenTheServiceWillNotAnswer() {
        final Broker broker = AmqpBrokerFailureTest.broker();
        if (broker.receive(AmqpBrokerFailureTest.QUEUE, Duration.ofMillis(100L)).isEmpty()) {
            throw new IllegalStateException("there was nothing to take");
        }
        Assertions.assertThrows(TransportException.class, broker::restore);
    }

    @Test
    void takesTheDeliveryTheServiceHandsOut() {
        Assertions.assertTrue(
            AmqpBrokerFailureTest.broker()
                .receive(AmqpBrokerFailureTest.QUEUE, Duration.ofMillis(100L))
                .isPresent()
        );
    }

    @Test
    void refusesToReceiveWhenTheServiceWillNotAnswer() {
        Assertions.assertThrows(
            TransportException.class,
            () -> new AmqpBroker(
                new RefusingChannel(null).channel(), new Exchange(Exchange.NONE, false)
            ).receive(AmqpBrokerFailureTest.QUEUE, Duration.ofMillis(100L))
        );
    }

    @Test
    void refusesToCloseWhenTheServiceTakesTooLong() {
        Assertions.assertThrows(
            TransportException.class,
            () -> new AmqpBroker(
                new RefusingChannel(AmqpBrokerFailureTest.message(), true).channel(),
                new Exchange(Exchange.NONE, false)
            ).close()
        );
    }

    private static Broker broker() {
        return new AmqpBroker(
            new RefusingChannel(AmqpBrokerFailureTest.message()).channel(),
            new Exchange(Exchange.NONE, false)
        );
    }

    private static Message message() {
        return new ProtocolV2().message(
            new Task("task-one", "proj.java.sum", List.of(2, 3), Map.of()),
            AmqpBrokerFailureTest.QUEUE
        );
    }
}
