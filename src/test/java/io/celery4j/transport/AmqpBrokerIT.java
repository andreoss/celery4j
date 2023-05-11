/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.transport;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import io.celery4j.protocol.MessageHeaders;
import java.io.IOException;
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
 * Live test case for {@link AmqpBroker}, against a message queue service in a
 * container.
 *
 * @since 0.2.0
 */
@Tag("live")
@Testcontainers
final class AmqpBrokerIT implements BrokerContract {

    /**
     * The service the cases run against.
     */
    @Container
    private static final RabbitMQContainer SERVICE = new RabbitMQContainer(
        DockerImageName.parse("docker.io/library/rabbitmq:3.13-alpine")
            .asCompatibleSubstituteFor("rabbitmq")
    );

    @Override
    public Broker broker() {
        return new AmqpBroker(AmqpBrokerIT.channel());
    }

    @Override
    public String queue() {
        return String.format("live-%s", UUID.randomUUID());
    }

    @Test
    void carriesTheTaskHeadersOfAMessage() {
        try (Broker broker = this.broker()) {
            final String queue = this.queue();
            broker.send(this.message("proj.tasks.add", queue), queue);
            Assertions.assertEquals(
                "proj.tasks.add",
                broker.receive(queue, BrokerContract.WAIT).orElseThrow().task()
            );
        }
    }

    @Test
    void carriesTheLanguageOfAProducer() {
        try (Broker broker = this.broker()) {
            final String queue = this.queue();
            broker.send(this.message("proj.tasks.add", queue), queue);
            Assertions.assertEquals(
                Optional.of("java"),
                broker.receive(queue, BrokerContract.WAIT)
                    .orElseThrow()
                    .headers()
                    .text(MessageHeaders.LANG)
            );
        }
    }

    @Test
    void namesTheQueueAMessageWasRoutedTo() {
        try (Broker broker = this.broker()) {
            final String queue = this.queue();
            broker.send(this.message("proj.tasks.add", queue), queue);
            Assertions.assertTrue(
                broker.receive(queue, BrokerContract.WAIT)
                    .orElseThrow()
                    .properties()
                    .asMap()
                    .toString()
                    .contains(queue)
            );
        }
    }

    private static Channel channel() {
        final ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(AmqpBrokerIT.SERVICE.getHost());
        factory.setPort(AmqpBrokerIT.SERVICE.getAmqpPort());
        factory.setUsername(AmqpBrokerIT.SERVICE.getAdminUsername());
        factory.setPassword(AmqpBrokerIT.SERVICE.getAdminPassword());
        try {
            return factory.newConnection().createChannel();
        } catch (final IOException ex) {
            throw new TransportException("the service cannot be reached", ex);
        } catch (final TimeoutException ex) {
            throw new TransportException("the service took too long to answer", ex);
        }
    }
}
