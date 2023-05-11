/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.transport;

import io.celery4j.protocol.JsonMessageCodec;
import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageProperties;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import redis.clients.jedis.JedisPool;

/**
 * Live test case for {@link RedisBroker}, against a store in a container.
 *
 * <p>The store announces the address it was given rather than being assumed at
 * a port, so that runs beside each other cannot collide.</p>
 *
 * @since 0.1.0
 */
@Tag("live")
@Testcontainers
final class RedisBrokerIT implements BrokerContract {

    /**
     * Port the store listens on inside its container.
     */
    private static final int PORT = 6379;

    /**
     * The store the cases run against.
     */
    @Container
    private static final GenericContainer<?> STORE =
        new GenericContainer<>("docker.io/library/redis:7-alpine")
            .withExposedPorts(RedisBrokerIT.PORT);

    @Override
    public Broker broker() {
        return new RedisBroker(
            new JedisPool(
                RedisBrokerIT.STORE.getHost(),
                RedisBrokerIT.STORE.getMappedPort(RedisBrokerIT.PORT)
            ),
            new JsonMessageCodec(),
            new Queues(),
            String.format("%s-%s", RedisBroker.HELD, UUID.randomUUID())
        );
    }

    @Override
    public String queue() {
        return "live";
    }

    @Test
    void sendsAPriorityToItsOwnQueue() {
        try (Broker broker = this.broker()) {
            final String queue = this.queue("priority");
            final Message plain = this.message("proj.tasks.add", queue);
            broker.send(
                new Message(
                    plain.properties().with(MessageProperties.PRIORITY, 6),
                    plain.headers(),
                    plain.body()
                ),
                queue
            );
            Assertions.assertTrue(
                broker.receive(new Queues().named(queue, 6L), Duration.ofSeconds(2)).isPresent()
            );
        }
    }

    @Test
    void reportsAnEmptyQueueWithinItsTimeout() {
        try (Broker broker = this.broker()) {
            Assertions.assertEquals(
                Optional.empty(), broker.receive(this.queue("timeout"), Duration.ofMillis(300))
            );
        }
    }
}
