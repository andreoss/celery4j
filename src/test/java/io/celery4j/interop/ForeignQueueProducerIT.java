/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.interop;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import io.celery4j.protocol.MessageHeaders;
import io.celery4j.protocol.Protocols;
import io.celery4j.result.FakeBackend;
import io.celery4j.result.State;
import io.celery4j.result.TaskResult;
import io.celery4j.transport.AmqpBroker;
import io.celery4j.transport.Broker;
import io.celery4j.transport.TransportException;
import io.celery4j.worker.Registry;
import io.celery4j.worker.Worker;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Live test case with a producer of the original implementation writing
 * through the message queue, and a worker of this library running what it
 * asked for.
 *
 * <p>The first transport has this direction covered. Over the queue service
 * only the other direction did, and the two transports carry a task
 * differently enough that one says little about the other.</p>
 *
 * @since 0.3.0
 */
@Tag("live")
@Testcontainers
final class ForeignQueueProducerIT {

    /**
     * Name of the task the foreign producer asks for.
     */
    private static final String ECHO = "proj.java.echo";

    /**
     * How long a receive waits.
     */
    private static final Duration WAIT = Duration.ofSeconds(10L);

    /**
     * Network the service and the producer meet on.
     */
    private static final Network NETWORK = Network.newNetwork();

    /**
     * The queue service both sides use.
     */
    @Container
    private static final RabbitMQContainer MESSAGES = ForeignQueueProducerIT.service();

    /**
     * The foreign side, holding the producer these cases run.
     */
    @Container
    private static final GenericContainer<?> FOREIGN = ForeignQueueProducerIT.foreign();

    @Test
    void runsATaskAForeignProducerQueued() {
        Assertions.assertEquals(
            Optional.of(5), ForeignQueueProducerIT.handled().value()
        );
    }

    @Test
    void marksAQueuedTaskAsSucceeded() {
        Assertions.assertEquals(
            new State(State.SUCCESS), ForeignQueueProducerIT.handled().state()
        );
    }

    @Test
    void readsTheParametersAForeignProducerQueued() {
        final String queue = ForeignQueueProducerIT.queue();
        ForeignQueueProducerIT.ask(queue);
        try (Broker broker = ForeignQueueProducerIT.broker()) {
            Assertions.assertEquals(
                List.of(2, 3),
                new Protocols()
                    .task(broker.receive(queue, ForeignQueueProducerIT.WAIT).orElseThrow())
                    .args()
            );
        }
    }

    @Test
    void readsTheLanguageAForeignProducerNamed() {
        final String queue = ForeignQueueProducerIT.queue();
        ForeignQueueProducerIT.ask(queue);
        try (Broker broker = ForeignQueueProducerIT.broker()) {
            Assertions.assertEquals(
                Optional.of("py"),
                broker.receive(queue, ForeignQueueProducerIT.WAIT)
                    .orElseThrow()
                    .headers()
                    .text(MessageHeaders.LANG)
            );
        }
    }

    @Test
    void keepsTheForeignSideRunning() {
        Assertions.assertTrue(ForeignQueueProducerIT.FOREIGN.isRunning());
    }

    private static TaskResult handled() {
        final String queue = ForeignQueueProducerIT.queue();
        ForeignQueueProducerIT.ask(queue);
        try (Broker broker = ForeignQueueProducerIT.broker()) {
            return new Worker(
                broker,
                new FakeBackend(new ConcurrentHashMap<>()),
                new Registry(
                    Map.of(
                        ForeignQueueProducerIT.ECHO,
                        task -> task.args().stream().mapToInt(arg -> (Integer) arg).sum()
                    )
                )
            ).once(queue, ForeignQueueProducerIT.WAIT).orElseThrow();
        }
    }

    private static void ask(final String queue) {
        try {
            ForeignQueueProducerIT.FOREIGN.execInContainer(
                "python",
                "producer.py",
                UUID.randomUUID().toString(),
                ForeignQueueProducerIT.ECHO,
                queue
            );
        } catch (final IOException ex) {
            throw new IllegalStateException("the foreign producer could not be run", ex);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("running the foreign producer was interrupted", ex);
        }
    }

    private static String queue() {
        return String.format("queued-%s", UUID.randomUUID());
    }

    private static Broker broker() {
        return new AmqpBroker(ForeignQueueProducerIT.channel());
    }

    private static RabbitMQContainer service() {
        return new RabbitMQContainer(ForeignQueueProducerIT.queueImage())
            .withNetwork(ForeignQueueProducerIT.NETWORK)
            .withNetworkAliases("mq");
    }

    private static DockerImageName queueImage() {
        return DockerImageName.parse("docker.io/library/rabbitmq:3.13-alpine")
            .asCompatibleSubstituteFor("rabbitmq");
    }

    private static GenericContainer<?> foreign() {
        return new GenericContainer<>(ForeignQueueProducerIT.image())
            .withNetwork(ForeignQueueProducerIT.NETWORK)
            .withEnv("BROKER_URL", "amqp://guest:guest@mq:5672//")
            .withEnv("RESULT_URL", "rpc://")
            .withStartupTimeout(Duration.ofMinutes(5L));
    }

    private static ImageFromDockerfile image() {
        return new ImageFromDockerfile()
            .withFileFromClasspath("Dockerfile", "live/Dockerfile")
            .withFileFromClasspath("tasks.py", "live/tasks.py")
            .withFileFromClasspath("producer.py", "live/producer.py")
            .withFileFromClasspath("schedule.py", "live/schedule.py")
            .withFileFromClasspath("serialize.py", "live/serialize.py")
            .withFileFromClasspath("read.py", "live/read.py")
            .withFileFromClasspath("workflow.py", "live/workflow.py");
    }

    private static Channel channel() {
        final ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(ForeignQueueProducerIT.MESSAGES.getHost());
        factory.setPort(ForeignQueueProducerIT.MESSAGES.getAmqpPort());
        factory.setUsername(ForeignQueueProducerIT.MESSAGES.getAdminUsername());
        factory.setPassword(ForeignQueueProducerIT.MESSAGES.getAdminPassword());
        try {
            return factory.newConnection().createChannel();
        } catch (final IOException ex) {
            throw new TransportException("the queue service cannot be reached", ex);
        } catch (final TimeoutException ex) {
            throw new TransportException("the queue service took too long to answer", ex);
        }
    }
}
