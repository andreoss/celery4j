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
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * What every broker promises, whichever store carries the queues.
 *
 * <p>Each case works on a queue of its own, so that what one case leaves
 * behind cannot be read by another.</p>
 *
 * @since 0.1.0
 */
interface BrokerContract {

    /**
     * How long a receive waits for a message that should be there.
     */
    Duration WAIT = Duration.ofSeconds(2);

    /**
     * How long a receive waits for a message that should not be.
     */
    Duration GLANCE = Duration.ofMillis(300);

    /**
     * A broker to test.
     *
     * @return The broker
     */
    Broker broker();

    /**
     * The name the queues of these cases start with.
     *
     * @return Name of the queue
     */
    String queue();

    /**
     * A broker delivers the headers it was sent.
     *
     * <p>The delivery properties are the transport's own business and may
     * differ: what must survive is what the message means.</p>
     */
    @Test
    default void deliversTheHeadersItWasSent() {
        try (Broker broker = this.broker()) {
            final String queue = this.queue("sent");
            final Message sent = this.message("proj.tasks.add", queue);
            broker.send(sent, queue);
            Assertions.assertEquals(
                sent.headers().asMap(),
                broker.receive(queue, BrokerContract.WAIT).orElseThrow().headers().asMap()
            );
        }
    }

    /**
     * A broker delivers the body it was sent.
     */
    @Test
    default void deliversTheBodyItWasSent() {
        try (Broker broker = this.broker()) {
            final String queue = this.queue("body");
            final Message sent = this.message("proj.tasks.add", queue);
            broker.send(sent, queue);
            Assertions.assertEquals(
                sent.body(), broker.receive(queue, BrokerContract.WAIT).orElseThrow().body()
            );
        }
    }

    /**
     * A broker reports an empty queue rather than waiting on.
     */
    @Test
    default void deliversNothingFromAnEmptyQueue() {
        try (Broker broker = this.broker()) {
            Assertions.assertEquals(
                Optional.empty(), broker.receive(this.queue("empty"), BrokerContract.GLANCE)
            );
        }
    }

    /**
     * A message sent to one queue is not delivered from another.
     */
    @Test
    default void keepsQueuesApart() {
        try (Broker broker = this.broker()) {
            final String queue = this.queue("apart");
            broker.send(this.message("proj.tasks.add", queue), queue);
            Assertions.assertEquals(
                Optional.empty(),
                broker.receive(this.queue("apart-other"), BrokerContract.GLANCE)
            );
        }
    }

    /**
     * A broker delivers messages in the order it took them.
     */
    @Test
    default void deliversInTheOrderItTookThem() {
        try (Broker broker = this.broker()) {
            final String queue = this.queue("order");
            broker.send(this.message("proj.tasks.first", queue), queue);
            broker.send(this.message("proj.tasks.second", queue), queue);
            Assertions.assertEquals(
                "proj.tasks.first",
                broker.receive(queue, BrokerContract.WAIT).orElseThrow().task()
            );
        }
    }

    /**
     * A message keeps the parameters of its task across the wire.
     */
    @Test
    default void keepsTheParametersOfATask() {
        try (Broker broker = this.broker()) {
            final String queue = this.queue("params");
            broker.send(this.message("proj.tasks.add", queue), queue);
            Assertions.assertEquals(
                List.of(2, 2),
                new ProtocolV2()
                    .task(broker.receive(queue, BrokerContract.WAIT).orElseThrow())
                    .args()
            );
        }
    }

    /**
     * A message taken but never seen through comes back.
     */
    @Test
    default void returnsWhatWasNeverSeenThrough() {
        try (Broker broker = this.broker()) {
            final String queue = this.queue("held");
            broker.send(this.message("proj.tasks.add", queue), queue);
            broker.receive(queue, BrokerContract.WAIT);
            broker.restore();
            Assertions.assertTrue(broker.receive(queue, BrokerContract.WAIT).isPresent());
        }
    }

    /**
     * A message that was seen through does not come back.
     */
    @Test
    default void keepsWhatWasSeenThrough() {
        try (Broker broker = this.broker()) {
            final String queue = this.queue("acked");
            broker.send(this.message("proj.tasks.add", queue), queue);
            broker.done(broker.receive(queue, BrokerContract.WAIT).orElseThrow());
            broker.restore();
            Assertions.assertEquals(
                Optional.empty(), broker.receive(queue, BrokerContract.GLANCE)
            );
        }
    }

    /**
     * Restoring counts what it returned.
     */
    @Test
    default void countsWhatItReturned() {
        try (Broker broker = this.broker()) {
            final String queue = this.queue("counted");
            broker.send(this.message("proj.tasks.add", queue), queue);
            broker.receive(queue, BrokerContract.WAIT);
            Assertions.assertEquals(1L, broker.restore());
        }
    }

    /**
     * Restoring an empty hold returns nothing.
     */
    @Test
    default void returnsNothingWhenItHoldsNothing() {
        try (Broker broker = this.broker()) {
            Assertions.assertEquals(0L, broker.restore());
        }
    }

    /**
     * The queue a case works on.
     *
     * @param suffix What tells this case apart from the others
     * @return Name of the queue
     */
    default String queue(final String suffix) {
        return String.format("%s-%s", this.queue(), suffix);
    }

    /**
     * A message asking for a task.
     *
     * @param name Name of the task
     * @param queue Queue it is routed to
     * @return The message
     */
    default Message message(final String name, final String queue) {
        return new ProtocolV2().message(
            new Task("task-one", name, List.of(2, 2), Map.of()), queue
        );
    }
}
