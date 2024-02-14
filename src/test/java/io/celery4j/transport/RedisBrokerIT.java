/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2024
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.transport;

import io.celery4j.protocol.JsonMessageCodec;
import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageHeaders;
import io.celery4j.protocol.MessageProperties;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
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

    @Test
    void refusesToLetGoOfAMessageWhenTheStoreIsGone() {
        final JedisPool pool = RedisBrokerIT.pool();
        final String queue = this.queue("gone");
        try (Broker broker = RedisBrokerIT.sharing(pool)) {
            broker.send(this.message("proj.tasks.add", queue), queue);
            final Message taken = broker.receive(queue, Duration.ofSeconds(2)).orElseThrow();
            pool.close();
            Assertions.assertThrows(TransportException.class, () -> broker.done(taken));
        }
    }

    @Test
    void stopsWaitingWhenTheThreadIsInterrupted() {
        try (Broker broker = this.broker()) {
            Thread.currentThread().interrupt();
            Assertions.assertThrows(
                TransportException.class,
                () -> broker.receive(this.queue("interrupted"), Duration.ofSeconds(2))
            );
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void refusesToSendAMessageItCannotWrite() {
        try (
            Broker broker = new RedisBroker(
                RedisBrokerIT.pool(),
                new RefusingCodec(),
                new Queues(),
                String.format("%s-%s", RedisBroker.HELD, UUID.randomUUID())
            )
        ) {
            Assertions.assertThrows(
                TransportException.class,
                () -> broker.send(this.message("proj.tasks.add", "refused"), "refused")
            );
        }
    }

    @Test
    void refusesToReadAMessageItCannotUnderstand() {
        final String queue = this.queue("unreadable");
        try (Broker broker = this.broker()) {
            broker.send(this.message("proj.tasks.add", queue), queue);
        }
        try (
            Broker broker = new RedisBroker(
                RedisBrokerIT.pool(),
                new RefusingCodec(),
                new Queues(),
                String.format("%s-%s", RedisBroker.HELD, UUID.randomUUID())
            )
        ) {
            Assertions.assertThrows(
                TransportException.class,
                () -> broker.receive(queue, Duration.ofSeconds(2L))
            );
        }
    }

    @Test
    void sendsBackAMessageThatNamesNoQueueToTheFirstOne() {
        Assertions.assertTrue(
            RedisBrokerIT.restored(null).receive("celery", Duration.ofSeconds(2L)).isPresent()
        );
    }

    @Test
    void sendsBackAMessageThatNamesNoTextToTheFirstOne() {
        Assertions.assertTrue(
            RedisBrokerIT.restored(Map.of(MessageProperties.ROUTING, 7))
                .receive("celery", Duration.ofSeconds(2L))
                .isPresent()
        );
    }

    private static Broker sharing(final JedisPool pool) {
        return new RedisBroker(
            pool,
            new JsonMessageCodec(),
            new Queues(),
            String.format("%s-%s", RedisBroker.HELD, UUID.randomUUID())
        );
    }

    private static Broker restored(final Map<String, Object> delivery) {
        final Broker broker = new RedisBroker(
            RedisBrokerIT.pool(),
            new JsonMessageCodec(),
            new Queues(),
            String.format("%s-%s", RedisBroker.HELD, UUID.randomUUID())
        );
        final String queue = String.format("plain-%s", UUID.randomUUID());
        broker.send(RedisBrokerIT.bare(delivery), queue);
        if (broker.receive(queue, Duration.ofSeconds(2L)).isEmpty()) {
            throw new IllegalStateException("there was nothing to hold");
        }
        broker.restore();
        return broker;
    }

    private static Message bare(final Map<String, Object> delivery) {
        final Map<String, Object> properties = new LinkedHashMap<>(
            Map.of(
                MessageProperties.TYPE, MessageProperties.JSON,
                MessageProperties.CORRELATION, UUID.randomUUID().toString()
            )
        );
        if (delivery != null) {
            properties.put(MessageProperties.DELIVERY, delivery);
        }
        return new Message(
            new MessageProperties(properties),
            new MessageHeaders(
                Map.of(
                    MessageHeaders.TASK, "proj.tasks.add",
                    MessageHeaders.ID, UUID.randomUUID().toString()
                )
            ),
            "W1syLCAzXSwge30sIHt9XQ=="
        );
    }

    private static JedisPool pool() {
        return new JedisPool(
            RedisBrokerIT.STORE.getHost(),
            RedisBrokerIT.STORE.getMappedPort(RedisBrokerIT.PORT)
        );
    }
}
