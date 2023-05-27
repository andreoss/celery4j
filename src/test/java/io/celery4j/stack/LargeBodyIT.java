/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.stack;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import io.celery4j.protocol.Message;
import io.celery4j.protocol.ProtocolV2;
import io.celery4j.protocol.Task;
import io.celery4j.transport.AmqpBroker;
import io.celery4j.transport.Broker;
import io.celery4j.transport.RedisBroker;
import io.celery4j.transport.TransportException;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
 * Live test case for a body far larger than any other case sends.
 *
 * <p>Every other case sends a handful of small values. A transport that
 * truncates, a codec that buffers badly or a limit nobody documented would all
 * show up here and nowhere else.</p>
 *
 * @since 0.3.0
 */
@Tag("live")
@Testcontainers
final class LargeBodyIT {

    /**
     * Port the store listens on inside its container.
     */
    private static final int PORT = 6379;

    /**
     * How long a receive waits.
     */
    private static final Duration WAIT = Duration.ofSeconds(5L);

    /**
     * How many items the large argument holds.
     */
    private static final int ITEMS = 20_000;

    /**
     * The store one of the transports runs against.
     */
    @Container
    private static final GenericContainer<?> STORE =
        new GenericContainer<>("docker.io/library/redis:7-alpine")
            .withExposedPorts(LargeBodyIT.PORT);

    /**
     * The queue service the other transport runs against.
     */
    @Container
    private static final RabbitMQContainer MESSAGES = new RabbitMQContainer(
        DockerImageName.parse("docker.io/library/rabbitmq:3.13-alpine")
            .asCompatibleSubstituteFor("rabbitmq")
    ).withStartupTimeout(Duration.ofMinutes(3L));

    @Test
    void carriesALargeBodyOverTheStore() {
        Assertions.assertEquals(
            LargeBodyIT.large(), LargeBodyIT.carried(LargeBodyIT.store())
        );
    }

    @Test
    void carriesALargeBodyOverTheQueueService() {
        Assertions.assertEquals(
            LargeBodyIT.large(), LargeBodyIT.carried(LargeBodyIT.queued())
        );
    }

    @Test
    void carriesLongTextOverTheStore() {
        Assertions.assertEquals(
            LargeBodyIT.text(), LargeBodyIT.echoed(LargeBodyIT.store(), LargeBodyIT.text())
        );
    }

    @Test
    void carriesLongTextOverTheQueueService() {
        Assertions.assertEquals(
            LargeBodyIT.text(), LargeBodyIT.echoed(LargeBodyIT.queued(), LargeBodyIT.text())
        );
    }

    private static Object carried(final Broker broker) {
        return LargeBodyIT.echoed(broker, LargeBodyIT.large());
    }

    private static Object echoed(final Broker broker, final Object argument) {
        final String queue = String.format("large-%s", UUID.randomUUID());
        try (Broker carrying = broker) {
            carrying.send(LargeBodyIT.message(queue, argument), queue);
            return new ProtocolV2()
                .task(carrying.receive(queue, LargeBodyIT.WAIT).orElseThrow())
                .args()
                .get(0);
        }
    }

    private static Message message(final String queue, final Object argument) {
        return new ProtocolV2().message(
            new Task(
                UUID.randomUUID().toString(), "proj.java.big", List.of(argument), Map.of()
            ),
            queue
        );
    }

    private static List<Object> large() {
        return IntStream.range(0, LargeBodyIT.ITEMS)
            .boxed()
            .map(Object.class::cast)
            .toList();
    }

    private static String text() {
        return IntStream.range(0, LargeBodyIT.ITEMS)
            .mapToObj(index -> "héllo")
            .collect(Collectors.joining());
    }

    private static Broker store() {
        return new RedisBroker(
            new JedisPool(
                LargeBodyIT.STORE.getHost(), LargeBodyIT.STORE.getMappedPort(LargeBodyIT.PORT)
            )
        );
    }

    private static Broker queued() {
        return new AmqpBroker(LargeBodyIT.channel());
    }

    private static Channel channel() {
        final ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(LargeBodyIT.MESSAGES.getHost());
        factory.setPort(LargeBodyIT.MESSAGES.getAmqpPort());
        factory.setUsername(LargeBodyIT.MESSAGES.getAdminUsername());
        factory.setPassword(LargeBodyIT.MESSAGES.getAdminPassword());
        try {
            return factory.newConnection().createChannel();
        } catch (final IOException ex) {
            throw new TransportException("the queue service cannot be reached", ex);
        } catch (final TimeoutException ex) {
            throw new TransportException("the queue service took too long to answer", ex);
        }
    }
}
