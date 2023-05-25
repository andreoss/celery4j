/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.interop;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import io.celery4j.protocol.ProtocolV2;
import io.celery4j.protocol.Task;
import io.celery4j.result.Backend;
import io.celery4j.result.RedisBackend;
import io.celery4j.result.Results;
import io.celery4j.result.State;
import io.celery4j.result.TaskResult;
import io.celery4j.transport.AmqpBroker;
import io.celery4j.transport.Broker;
import io.celery4j.transport.Exchange;
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
import org.testcontainers.containers.Network;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.JedisPool;

/**
 * Live test case for reaching a foreign worker the way the framework routes
 * to it: through the exchange it declared, by the key it bound its queue
 * with.
 *
 * <p>Writing to the nameless exchange reaches the same queue and says nothing
 * about routing, because the service delivers by queue name there whatever
 * anybody declared. Only a message published to the exchange the other side
 * declared, under the name it gave it, and left to the binding to deliver,
 * shows that this library can take part in a topology it did not invent.</p>
 *
 * @since 1.0.0
 */
@Tag("live")
@Testcontainers
final class ForeignExchangeIT {

    /**
     * Port the result store listens on inside its container.
     */
    private static final int PORT = 6379;

    /**
     * Queue the foreign worker reads.
     */
    private static final String QUEUE = "interop";

    /**
     * Name of the task the foreign worker answers for.
     */
    private static final String ADD = "proj.tasks.add";

    /**
     * How long a task may take to be run by the other side.
     */
    private static final Duration PATIENCE = Duration.ofSeconds(60L);

    /**
     * Network the service, the store and the worker meet on.
     */
    private static final Network NETWORK = Network.newNetwork();

    /**
     * The queue service both sides use.
     */
    @Container
    private static final RabbitMQContainer MESSAGES = ForeignExchangeIT.service();

    /**
     * The store the results go to.
     */
    @Container
    private static final GenericContainer<?> STORE =
        new GenericContainer<>("docker.io/library/redis:7-alpine")
            .withExposedPorts(ForeignExchangeIT.PORT)
            .withNetwork(ForeignExchangeIT.NETWORK)
            .withNetworkAliases("store");

    /**
     * A worker of the original implementation.
     */
    @Container
    private static final GenericContainer<?> FOREIGN = ForeignExchangeIT.foreign();

    @Test
    void hasATaskRoutedThroughTheForeignExchangeRun() {
        Assertions.assertEquals(Optional.of(7), ForeignExchangeIT.routed(List.of(2, 5)).value());
    }

    @Test
    void marksATaskRoutedThroughTheForeignExchangeAsSucceeded() {
        Assertions.assertEquals(
            new State(State.SUCCESS), ForeignExchangeIT.routed(List.of(1, 1)).state()
        );
    }

    @Test
    void writesToTheQueueTheOtherSideDeclaredWithoutDeclaringIt() {
        Assertions.assertEquals(Optional.of(4), ForeignExchangeIT.written(List.of(2, 2)).value());
    }

    @Test
    void keepsAForeignWorkerRunning() {
        Assertions.assertTrue(ForeignExchangeIT.FOREIGN.isRunning());
    }

    private static TaskResult routed(final List<Object> args) {
        final String id = UUID.randomUUID().toString();
        try (
            Broker broker = new AmqpBroker(
                ForeignExchangeIT.channel(), new Exchange(ForeignExchangeIT.bound())
            )
        ) {
            broker.send(
                new ProtocolV2().message(
                    new Task(id, ForeignExchangeIT.ADD, args, Map.of()),
                    ForeignExchangeIT.QUEUE
                ),
                ForeignExchangeIT.QUEUE
            );
        }
        return ForeignExchangeIT.finished(id);
    }

    private static TaskResult finished(final String id) {
        try (Backend backend = new RedisBackend(ForeignExchangeIT.pool())) {
            return new Results(backend, Duration.ofMillis(200L))
                .await(id, ForeignExchangeIT.PATIENCE)
                .orElseThrow();
        }
    }

    private static TaskResult written(final List<Object> args) {
        final String id = UUID.randomUUID().toString();
        try (
            Broker broker = new AmqpBroker(
                ForeignExchangeIT.channel(), new Exchange(Exchange.NONE, false)
            )
        ) {
            broker.send(
                new ProtocolV2().message(
                    new Task(id, ForeignExchangeIT.ADD, args, Map.of()),
                    ForeignExchangeIT.QUEUE
                ),
                ForeignExchangeIT.QUEUE
            );
        }
        return ForeignExchangeIT.finished(id);
    }

    private static String bound() {
        final String exchange = String.format("routes-%s", UUID.randomUUID());
        try {
            final Channel channel = ForeignExchangeIT.channel();
            channel.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT, true);
            channel.queueBind(ForeignExchangeIT.QUEUE, exchange, ForeignExchangeIT.QUEUE);
            channel.close();
        } catch (final IOException ex) {
            throw new TransportException("the topology cannot be declared", ex);
        } catch (final TimeoutException ex) {
            throw new TransportException("declaring the topology took too long", ex);
        }
        return exchange;
    }

    private static RabbitMQContainer service() {
        return new RabbitMQContainer(ForeignExchangeIT.queueImage())
            .withNetwork(ForeignExchangeIT.NETWORK)
            .withNetworkAliases("mq");
    }

    private static DockerImageName queueImage() {
        return DockerImageName.parse("docker.io/library/rabbitmq:3.13-alpine")
            .asCompatibleSubstituteFor("rabbitmq");
    }

    private static GenericContainer<?> foreign() {
        return new GenericContainer<>(ForeignExchangeIT.image())
            .withNetwork(ForeignExchangeIT.NETWORK)
            .withEnv("BROKER_URL", "amqp://guest:guest@mq:5672//")
            .withEnv("RESULT_URL", "redis://store:6379/0")
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
            .withFileFromClasspath("workflow.py", "live/workflow.py")
            .withFileFromClasspath("header.py", "live/header.py");
    }

    private static Channel channel() {
        final ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(ForeignExchangeIT.MESSAGES.getHost());
        factory.setPort(ForeignExchangeIT.MESSAGES.getAmqpPort());
        factory.setUsername(ForeignExchangeIT.MESSAGES.getAdminUsername());
        factory.setPassword(ForeignExchangeIT.MESSAGES.getAdminPassword());
        try {
            return factory.newConnection().createChannel();
        } catch (final IOException ex) {
            throw new TransportException("the queue service cannot be reached", ex);
        } catch (final TimeoutException ex) {
            throw new TransportException("the queue service took too long to answer", ex);
        }
    }

    private static JedisPool pool() {
        return new JedisPool(
            ForeignExchangeIT.STORE.getHost(),
            ForeignExchangeIT.STORE.getMappedPort(ForeignExchangeIT.PORT)
        );
    }
}
