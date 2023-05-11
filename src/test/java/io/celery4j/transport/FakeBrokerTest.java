/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.transport;

import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageProperties;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link FakeBroker}, which also checks the broker contract.
 *
 * @since 0.1.0
 */
final class FakeBrokerTest implements BrokerContract {

    @Override
    public Broker broker() {
        return new FakeBroker(new ConcurrentHashMap<>(), new Queues(), new ConcurrentHashMap<>());
    }

    @Override
    public String queue() {
        return "important";
    }

    @Test
    void sendsAPriorityToItsOwnQueue() {
        try (Broker broker = this.broker()) {
            final Message plain = this.message("proj.tasks.add", this.queue());
            broker.send(
                new Message(
                    plain.properties().with(MessageProperties.PRIORITY, 6),
                    plain.headers(),
                    plain.body()
                ),
                this.queue()
            );
            Assertions.assertTrue(
                broker.receive(new Queues().named(this.queue(), 6L), Duration.ofMillis(100))
                    .isPresent()
            );
        }
    }

    @Test
    void forgetsEverythingWhenClosed() {
        final Broker broker = this.broker();
        broker.send(this.message("proj.tasks.add", this.queue()), this.queue());
        broker.close();
        Assertions.assertEquals(
            Optional.empty(), broker.receive(this.queue(), Duration.ofMillis(100))
        );
    }
}
