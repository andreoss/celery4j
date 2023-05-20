/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.stack;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import io.celery4j.protocol.JsonMessageCodec;
import io.celery4j.protocol.Message;
import io.celery4j.protocol.ProtocolV2;
import io.celery4j.protocol.Task;
import io.celery4j.transport.AmqpBroker;
import io.celery4j.transport.Broker;
import io.celery4j.transport.Queues;
import io.celery4j.transport.RedisBroker;
import io.celery4j.transport.TransportException;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.JedisPool;

/**
 * Live test case for what a worker that vanishes leaves behind.
 *
 * <p>Two promises are checked here that nothing else checks. The queue service
 * returns a delivery nobody acknowledged once the connection is gone, without
 * anyone asking it to. The store returns nothing by itself, so a message held
 * by a worker that never finished comes back only when another worker
 * restores, which is why they are named rather than counted.</p>
 *
 * @since 0.3.1
 */
@Tag("live")
@Testcontainers
final class RecoveryIT {

    /**
     * Port the store listens on inside its container.
     */
    private static final int PORT = 6379;

    /**
     * How long a receive waits.
     */
    private static final Duration WAIT = Duration.ofSeconds(3L);

    /**
     * The store one half of the cases run against.
     */
    @Container
    private static final GenericContainer<?> STORE =
        new GenericContainer<>("docker.io/library/redis:7-alpine")
            .withExposedPorts(RecoveryIT.PORT);

    /**
     * The queue service the other half run against.
     */
    @Container
    private static final RabbitMQContainer MESSAGES = new RabbitMQContainer(
        DockerImageName.parse("docker.io/library/rabbitmq:3.13-alpine")
            .asCompatibleSubstituteFor("rabbitmq")
    );

    @Test
    void returnsAnUnacknowledgedDeliveryWhenTheConnectionIsLost() {
        final String queue = RecoveryIT.queue();
        RecoveryIT.queued(queue);
        RecoveryIT.taken(queue);
        Assertions.assertTrue(RecoveryIT.again(queue).isPresent());
    }

    @Test
    void keepsADeliveryThatWasAcknowledged() {
        final String queue = RecoveryIT.queue();
        RecoveryIT.queued(queue);
        RecoveryIT.finished(queue);
        Assertions.assertTrue(RecoveryIT.again(queue).isEmpty());
    }

    @Test
    void returnsAHeldMessageToAnotherWorker() {
        final String queue = RecoveryIT.queue();
        final String held = RecoveryIT.held();
        RecoveryIT.stored(queue);
        RecoveryIT.vanished(queue, held);
        try (Broker other = RecoveryIT.store(held)) {
            other.restore();
            Assertions.assertTrue(other.receive(queue, RecoveryIT.WAIT).isPresent());
        }
    }

    @Test
    void countsWhatAnotherWorkerRestored() {
        final String queue = RecoveryIT.queue();
        final String held = RecoveryIT.held();
        RecoveryIT.stored(queue);
        RecoveryIT.vanished(queue, held);
        try (Broker other = RecoveryIT.store(held)) {
            Assertions.assertEquals(1L, other.restore());
        }
    }

    @Test
    void returnsNothingThatWasSeenThrough() {
        final String queue = RecoveryIT.queue();
        final String held = RecoveryIT.held();
        RecoveryIT.stored(queue);
        try (Broker worker = RecoveryIT.store(held)) {
            worker.done(worker.receive(queue, RecoveryIT.WAIT).orElseThrow());
        }
        try (Broker other = RecoveryIT.store(held)) {
            Assertions.assertEquals(0L, other.restore());
        }
    }

    private static void vanished(final String queue, final String held) {
        try (Broker worker = RecoveryIT.store(held)) {
            if (worker.receive(queue, RecoveryIT.WAIT).isEmpty()) {
                throw new IllegalStateException("there was nothing to hold");
            }
        }
    }

    private static void stored(final String queue) {
        try (Broker broker = RecoveryIT.store(RecoveryIT.held())) {
            broker.send(RecoveryIT.message(queue), queue);
        }
    }

    private static void queued(final String queue) {
        try (Broker broker = new AmqpBroker(RecoveryIT.channel())) {
            broker.send(RecoveryIT.message(queue), queue);
        }
    }

    private static void taken(final String queue) {
        try (Broker broker = new AmqpBroker(RecoveryIT.channel())) {
            if (broker.receive(queue, RecoveryIT.WAIT).isEmpty()) {
                throw new IllegalStateException("there was nothing to take");
            }
        }
    }

    private static void finished(final String queue) {
        try (Broker broker = new AmqpBroker(RecoveryIT.channel())) {
            broker.done(broker.receive(queue, RecoveryIT.WAIT).orElseThrow());
        }
    }

    private static Optional<Message> again(final String queue) {
        try (Broker broker = new AmqpBroker(RecoveryIT.channel())) {
            return broker.receive(queue, RecoveryIT.WAIT);
        }
    }

    private static Broker store(final String held) {
        return new RedisBroker(
            new JedisPool(
                RecoveryIT.STORE.getHost(), RecoveryIT.STORE.getMappedPort(RecoveryIT.PORT)
            ),
            new JsonMessageCodec(),
            new Queues(),
            held
        );
    }

    private static Message message(final String queue) {
        return new ProtocolV2().message(
            new Task(UUID.randomUUID().toString(), "proj.java.sum", List.of(2, 3), Map.of()),
            queue
        );
    }

    private static String queue() {
        return String.format("recovery-%s", UUID.randomUUID());
    }

    private static String held() {
        return String.format("held-%s", UUID.randomUUID());
    }

    private static Channel channel() {
        final ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RecoveryIT.MESSAGES.getHost());
        factory.setPort(RecoveryIT.MESSAGES.getAmqpPort());
        factory.setUsername(RecoveryIT.MESSAGES.getAdminUsername());
        factory.setPassword(RecoveryIT.MESSAGES.getAdminPassword());
        try {
            return factory.newConnection().createChannel();
        } catch (final IOException ex) {
            throw new TransportException("the queue service cannot be reached", ex);
        } catch (final TimeoutException ex) {
            throw new TransportException("the queue service took too long to answer", ex);
        }
    }
}
