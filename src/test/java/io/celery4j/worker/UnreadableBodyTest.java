/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.worker;

import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageHeaders;
import io.celery4j.protocol.MessageProperties;
import io.celery4j.protocol.ProtocolException;
import io.celery4j.result.Backend;
import io.celery4j.result.FakeBackend;
import io.celery4j.transport.Broker;
import io.celery4j.transport.FakeBroker;
import io.celery4j.transport.Queues;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for a worker handed a body written in a form it does not know.
 *
 * <p>Refusing such a message is checked where the codecs are. What matters
 * here is what is left behind afterwards: a message nobody could read must
 * stay where a consumer that can read it will find it, and must leave no
 * result claiming anything about the task.</p>
 *
 * @since 1.0.0
 */
final class UnreadableBodyTest {

    /**
     * Queue the message comes from.
     */
    private static final String QUEUE = "strange";

    /**
     * Identity of the task nobody here can read.
     */
    private static final String ID = "task-one";

    /**
     * How long a receive waits.
     */
    private static final Duration WAIT = Duration.ofMillis(50L);

    @Test
    void refusesABodyItCannotRead() {
        final Broker broker = UnreadableBodyTest.queued();
        Assertions.assertThrows(
            ProtocolException.class,
            () -> UnreadableBodyTest.worker(broker)
                .once(UnreadableBodyTest.QUEUE, UnreadableBodyTest.WAIT)
        );
    }

    @Test
    void namesTheFormItCannotRead() {
        final Broker broker = UnreadableBodyTest.queued();
        Assertions.assertTrue(
            Assertions.assertThrows(
                ProtocolException.class,
                () -> UnreadableBodyTest.worker(broker)
                    .once(UnreadableBodyTest.QUEUE, UnreadableBodyTest.WAIT)
            ).getMessage().contains("application/x-yaml")
        );
    }

    @Test
    void leavesTheMessageUnacknowledged() {
        Assertions.assertEquals(1L, UnreadableBodyTest.refused().restore());
    }

    @Test
    void leavesTheMessageWhereAnotherConsumerFindsIt() {
        final Broker broker = UnreadableBodyTest.refused();
        broker.restore();
        Assertions.assertEquals(
            UnreadableBodyTest.message(),
            broker.receive(UnreadableBodyTest.QUEUE, UnreadableBodyTest.WAIT).orElseThrow()
        );
    }

    @Test
    void leavesNoResultBehind() {
        final Backend backend = new FakeBackend(new ConcurrentHashMap<>());
        final Broker broker = UnreadableBodyTest.queued();
        Assertions.assertThrows(
            ProtocolException.class,
            () -> new Worker(broker, backend, new Registry(Map.of()))
                .once(UnreadableBodyTest.QUEUE, UnreadableBodyTest.WAIT)
        );
        Assertions.assertEquals(Optional.empty(), backend.of(UnreadableBodyTest.ID));
    }

    private static Broker refused() {
        final Broker broker = UnreadableBodyTest.queued();
        try {
            UnreadableBodyTest.worker(broker)
                .once(UnreadableBodyTest.QUEUE, UnreadableBodyTest.WAIT);
        } catch (final ProtocolException ex) {
            Assertions.assertTrue(ex.getMessage().contains("x-yaml"));
        }
        return broker;
    }

    private static Worker worker(final Broker broker) {
        return new Worker(
            broker, new FakeBackend(new ConcurrentHashMap<>()), new Registry(Map.of())
        );
    }

    private static Broker queued() {
        final Broker broker = new FakeBroker(
            new ConcurrentHashMap<>(), new Queues(), new ConcurrentHashMap<>()
        );
        broker.send(UnreadableBodyTest.message(), UnreadableBodyTest.QUEUE);
        return broker;
    }

    private static Message message() {
        return new Message(
            new MessageProperties(
                Map.of(
                    MessageProperties.CORRELATION, UnreadableBodyTest.ID,
                    MessageProperties.TYPE, "application/x-yaml",
                    MessageProperties.ENCODING, "utf-8",
                    MessageProperties.DELIVERY,
                    Map.of(MessageProperties.ROUTING, UnreadableBodyTest.QUEUE)
                )
            ),
            new MessageHeaders(
                Map.of(
                    MessageHeaders.ID, UnreadableBodyTest.ID,
                    MessageHeaders.TASK, "proj.tasks.add"
                )
            ),
            String.format("- [2, 3]%n- {}%n- {}%n")
        );
    }
}
