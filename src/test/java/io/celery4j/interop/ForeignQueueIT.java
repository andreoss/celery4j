/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.interop;

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
 * Live test case with a worker of the original implementation on the other end
 * of a message queue, rather than a key value store.
 *
 * <p>This is the same evidence the first transport got: the other side is the
 * software that defines the protocol, and it neither knows nor cares which
 * library wrote the message it is running.</p>
 *
 * @since 0.2.0
 */
@Tag("live")
@Testcontainers
final class ForeignQueueIT {

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
     * How long a foreign worker may take to start and finish a task.
     */
    private static final Duration PATIENCE = Duration.ofMinutes(2L);

    /**
     * Network the services and the worker meet on.
     */
    private static final Network NETWORK = Network.newNetwork();

    /**
     * The queue service both sides use.
     */
    @Container
    private static final RabbitMQContainer MESSAGES = ForeignQueueIT.service();

    /**
     * The store the results go to.
     */
    @Container
    private static final GenericContainer<?> STORE =
        new GenericContainer<>("docker.io/library/redis:7-alpine")
            .withExposedPorts(ForeignQueueIT.PORT)
            .withNetwork(ForeignQueueIT.NETWORK)
            .withNetworkAliases("store");

    /**
     * A worker of the original implementation.
     */
    @Container
    private static final GenericContainer<?> FOREIGN = ForeignQueueIT.foreign();

    @Test
    void hasItsTaskRunByAForeignWorkerOverTheQueue() {
        final String id = UUID.randomUUID().toString();
        ForeignQueueIT.send(id, ForeignQueueIT.ADD, List.of(2, 5));
        Assertions.assertEquals(Optional.of(7), ForeignQueueIT.finished(id).value());
    }

    @Test
    void readsTheStateAForeignWorkerWroteOverTheQueue() {
        final String id = UUID.randomUUID().toString();
        ForeignQueueIT.send(id, ForeignQueueIT.ADD, List.of(1, 1));
        Assertions.assertEquals(
            new State(State.SUCCESS), ForeignQueueIT.finished(id).state()
        );
    }

    @Test
    void readsTheFailureAForeignWorkerWroteOverTheQueue() {
        final String id = UUID.randomUUID().toString();
        ForeignQueueIT.send(id, "proj.tasks.boom", List.of());
        Assertions.assertEquals(
            new State(State.FAILURE), ForeignQueueIT.finished(id).state()
        );
    }

    @Test
    void keepsAForeignWorkerRunning() {
        Assertions.assertTrue(ForeignQueueIT.FOREIGN.isRunning());
    }

    private static void send(final String id, final String name, final List<Object> args) {
        try (Broker broker = new AmqpBroker(ForeignQueueIT.channel())) {
            broker.send(
                new ProtocolV2().message(
                    new Task(id, name, args, Map.of()), ForeignQueueIT.QUEUE
                ),
                ForeignQueueIT.QUEUE
            );
        }
    }

    private static TaskResult finished(final String id) {
        try (Backend backend = new RedisBackend(ForeignQueueIT.pool())) {
            return new Results(backend, Duration.ofMillis(200L))
                .await(id, ForeignQueueIT.PATIENCE)
                .orElseThrow();
        }
    }

    private static RabbitMQContainer service() {
        return new RabbitMQContainer(ForeignQueueIT.queueImage())
            .withNetwork(ForeignQueueIT.NETWORK)
            .withNetworkAliases("mq");
    }

    private static DockerImageName queueImage() {
        return DockerImageName.parse("docker.io/library/rabbitmq:3.13-alpine")
            .asCompatibleSubstituteFor("rabbitmq");
    }

    private static GenericContainer<?> foreign() {
        return new GenericContainer<>(ForeignQueueIT.image())
            .withNetwork(ForeignQueueIT.NETWORK)
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
            .withFileFromClasspath("read.py", "live/read.py");
    }

    private static Channel channel() {
        final ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(ForeignQueueIT.MESSAGES.getHost());
        factory.setPort(ForeignQueueIT.MESSAGES.getAmqpPort());
        factory.setUsername(ForeignQueueIT.MESSAGES.getAdminUsername());
        factory.setPassword(ForeignQueueIT.MESSAGES.getAdminPassword());
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
            ForeignQueueIT.STORE.getHost(),
            ForeignQueueIT.STORE.getMappedPort(ForeignQueueIT.PORT)
        );
    }
}
