/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.transport;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageProperties;
import io.celery4j.protocol.ProtocolV2;
import io.celery4j.protocol.Task;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
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
 * Live test case for a priority the queue service itself honours.
 *
 * <p>The store has no idea what a priority is, so a broker writing to it puts
 * each step on a queue of its own and reads the higher ones first. This
 * service does know: a queue declared to hold priorities serves what is more
 * urgent before what has been waiting, and a broker that split the queue by
 * name here would be writing where nobody is listening.</p>
 *
 * @since 1.0.0
 */
@Tag("live")
@Testcontainers
final class PriorityIT {

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
    );

    @Test
    void servesWhatIsMoreUrgentFirst() {
        Assertions.assertEquals(List.of("urgent"), PriorityIT.served().get(0).args());
    }

    @Test
    void servesWhatWasWaitingAfterwards() {
        Assertions.assertEquals(List.of("ordinary"), PriorityIT.served().get(1).args());
    }

    @Test
    void carriesThePriorityInTheDelivery() {
        Assertions.assertEquals(
            9L,
            PriorityIT.taken()
                .get(0)
                .properties()
                .number(MessageProperties.PRIORITY, 0L)
        );
    }

    @Test
    void writesEveryPriorityToTheQueueItWasAddressedTo() {
        Assertions.assertEquals(2, PriorityIT.taken().size());
    }

    private static List<Task> served() {
        return PriorityIT.taken().stream().map(new ProtocolV2()::task).toList();
    }

    private static List<Message> taken() {
        final String queue = PriorityIT.declared();
        try (
            Broker broker = new AmqpBroker(
                PriorityIT.channel(), new Exchange(Exchange.NONE, false)
            )
        ) {
            broker.send(PriorityIT.message(queue, "ordinary", 1L), queue);
            broker.send(PriorityIT.message(queue, "urgent", 9L), queue);
            return List.of(
                broker.receive(queue, PriorityIT.WAIT).orElseThrow(),
                broker.receive(queue, PriorityIT.WAIT).orElseThrow()
            );
        }
    }

    private static Message message(final String queue, final String what, final long priority) {
        final Message written = new ProtocolV2().message(
            new Task(UUID.randomUUID().toString(), "proj.java.sum", List.of(what), Map.of()),
            queue
        );
        return new Message(
            written.properties().with(MessageProperties.PRIORITY, (int) priority),
            written.headers(),
            written.body()
        );
    }

    private static String declared() {
        final String queue = String.format("urgent-%s", UUID.randomUUID());
        try {
            final Channel channel = PriorityIT.channel();
            channel.queueDeclare(queue, true, false, false, Map.of("x-max-priority", 10));
            channel.close();
        } catch (final IOException ex) {
            throw new TransportException("the queue cannot be declared", ex);
        } catch (final TimeoutException ex) {
            throw new TransportException("declaring the queue took too long", ex);
        }
        return queue;
    }

    private static Channel channel() {
        final ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(PriorityIT.MESSAGES.getHost());
        factory.setPort(PriorityIT.MESSAGES.getAmqpPort());
        factory.setUsername(PriorityIT.MESSAGES.getAdminUsername());
        factory.setPassword(PriorityIT.MESSAGES.getAdminPassword());
        try {
            return factory.newConnection().createChannel();
        } catch (final IOException ex) {
            throw new TransportException("the queue service cannot be reached", ex);
        } catch (final TimeoutException ex) {
            throw new TransportException("the queue service took too long to answer", ex);
        }
    }
}
