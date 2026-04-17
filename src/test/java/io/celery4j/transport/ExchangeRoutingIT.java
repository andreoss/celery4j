/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.transport;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import io.celery4j.protocol.Message;
import io.celery4j.protocol.ProtocolV2;
import io.celery4j.protocol.Task;
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
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Live test case for publishing through an exchange somebody else declared.
 *
 * <p>Every other case here writes to the nameless exchange and lets the broker
 * declare the queue, which is the one arrangement no deployment uses for long.
 * A deployment declares its exchanges, its queues and the bindings between
 * them, often with arguments a library has never heard of, and a broker that
 * insists on declaring them again is refused by the service.</p>
 *
 * @since 1.0.0
 */
@Tag("live")
@Testcontainers
final class ExchangeRoutingIT {

    /**
     * How long a receive waits.
     */
    private static final Duration WAIT = Duration.ofSeconds(3L);

    /**
     * The queue service the cases run against.
     */
    @Container
    private static final RabbitMQContainer MESSAGES = new RabbitMQContainer(
        DockerImageName.parse("docker.io/library/rabbitmq:3.13-alpine")
            .asCompatibleSubstituteFor("rabbitmq")
    ).withStartupTimeout(Duration.ofMinutes(3L));

    @Test
    void carriesATaskThroughANamedExchange() {
        final String queue = ExchangeRoutingIT.queue();
        try (
            Broker broker = new AmqpBroker(
                ExchangeRoutingIT.channel(), new Exchange(ExchangeRoutingIT.routed(queue, queue))
            )
        ) {
            broker.send(ExchangeRoutingIT.message(queue), queue);
            Assertions.assertEquals(
                List.of(2, 3),
                new ProtocolV2()
                    .task(broker.receive(queue, ExchangeRoutingIT.WAIT).orElseThrow())
                    .args()
            );
        }
    }

    @Test
    void routesByTheKeyTheBindingNames() {
        final String queue = ExchangeRoutingIT.queue();
        try (
            Broker broker = new AmqpBroker(
                ExchangeRoutingIT.channel(),
                new Exchange(ExchangeRoutingIT.routed(queue, "nothing-is-bound-to-this"))
            )
        ) {
            broker.send(ExchangeRoutingIT.message(queue), queue);
            Assertions.assertEquals(
                Optional.empty(), broker.receive(queue, ExchangeRoutingIT.WAIT)
            );
        }
    }

    @Test
    void writesToAQueueSomebodyElseDeclared() {
        final String queue = ExchangeRoutingIT.declared();
        try (
            Broker broker = new AmqpBroker(
                ExchangeRoutingIT.channel(), new Exchange(Exchange.NONE, false)
            )
        ) {
            broker.send(ExchangeRoutingIT.message(queue), queue);
            Assertions.assertTrue(broker.receive(queue, ExchangeRoutingIT.WAIT).isPresent());
        }
    }

    @Test
    void refusesToDeclareAQueueSomebodyElseDeclaredDifferently() {
        final String queue = ExchangeRoutingIT.declared();
        try (Broker broker = new AmqpBroker(ExchangeRoutingIT.channel())) {
            Assertions.assertThrows(
                TransportException.class,
                () -> broker.send(ExchangeRoutingIT.message(queue), queue)
            );
        }
    }

    private static String routed(final String queue, final String key) {
        final String exchange = String.format("routes-%s", UUID.randomUUID());
        try {
            final Channel channel = ExchangeRoutingIT.channel();
            channel.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT, true);
            channel.queueDeclare(queue, true, false, false, Map.of());
            channel.queueBind(queue, exchange, key);
            channel.close();
        } catch (final IOException ex) {
            throw new TransportException("the topology cannot be declared", ex);
        } catch (final TimeoutException ex) {
            throw new TransportException("declaring the topology took too long", ex);
        }
        return exchange;
    }

    private static String declared() {
        final String queue = ExchangeRoutingIT.queue();
        try {
            final Channel channel = ExchangeRoutingIT.channel();
            channel.queueDeclare(queue, true, false, false, Map.of("x-max-priority", 10));
            channel.close();
        } catch (final IOException ex) {
            throw new TransportException("the queue cannot be declared", ex);
        } catch (final TimeoutException ex) {
            throw new TransportException("declaring the queue took too long", ex);
        }
        return queue;
    }

    private static Message message(final String queue) {
        return new ProtocolV2().message(
            new Task(UUID.randomUUID().toString(), "proj.java.sum", List.of(2, 3), Map.of()),
            queue
        );
    }

    private static String queue() {
        return String.format("routed-%s", UUID.randomUUID());
    }

    private static Channel channel() {
        final ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(ExchangeRoutingIT.MESSAGES.getHost());
        factory.setPort(ExchangeRoutingIT.MESSAGES.getAmqpPort());
        factory.setUsername(ExchangeRoutingIT.MESSAGES.getAdminUsername());
        factory.setPassword(ExchangeRoutingIT.MESSAGES.getAdminPassword());
        try {
            return factory.newConnection().createChannel();
        } catch (final IOException ex) {
            throw new TransportException("the queue service cannot be reached", ex);
        } catch (final TimeoutException ex) {
            throw new TransportException("the queue service took too long to answer", ex);
        }
    }
}
