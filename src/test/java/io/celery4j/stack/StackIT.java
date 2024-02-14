/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2024
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.stack;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import io.celery4j.client.Client;
import io.celery4j.client.Handle;
import io.celery4j.client.Routes;
import io.celery4j.protocol.ProtocolV2;
import io.celery4j.result.Backend;
import io.celery4j.result.RedisBackend;
import io.celery4j.transport.AmqpBroker;
import io.celery4j.transport.Broker;
import io.celery4j.transport.RedisBroker;
import io.celery4j.transport.TransportException;
import io.celery4j.worker.Fleet;
import io.celery4j.worker.Registry;
import io.celery4j.worker.Worker;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * Live test case with every layer of this library in the path at once: a
 * caller asking through the client, a real broker carrying the message, a
 * fleet of workers running it, and a real store holding what it returned.
 *
 * <p>Each layer has its own cases elsewhere. These are the ones that would
 * fail if two layers disagreed with each other while each was right on its
 * own.</p>
 *
 * @since 0.2.1
 */
@Tag("live")
@Testcontainers
final class StackIT {

    /**
     * Port the store listens on inside its container.
     */
    private static final int PORT = 6379;

    /**
     * Name of the task these cases run.
     */
    private static final String NAME = "proj.java.sum";

    /**
     * How long a caller waits for an answer.
     */
    private static final Duration PATIENCE = Duration.ofSeconds(30L);

    /**
     * How long a receive waits.
     */
    private static final Duration WAIT = Duration.ofMillis(100L);

    /**
     * The store the results go to.
     */
    @Container
    private static final GenericContainer<?> STORE =
        new GenericContainer<>("docker.io/library/redis:7-alpine")
            .withExposedPorts(StackIT.PORT);

    /**
     * The queue service the other cases run against.
     */
    @Container
    private static final RabbitMQContainer MESSAGES = new RabbitMQContainer(
        DockerImageName.parse("docker.io/library/rabbitmq:3.13-alpine")
            .asCompatibleSubstituteFor("rabbitmq")
    ).withStartupTimeout(Duration.ofMinutes(3L));

    @Test
    void answersACallerOverTheStore() {
        Assertions.assertEquals(
            Optional.of(5), StackIT.asked(StackIT.store(), StackIT.queue("store"))
        );
    }

    @Test
    void answersACallerOverTheQueueService() {
        Assertions.assertEquals(
            Optional.of(5), StackIT.asked(StackIT.queued(), StackIT.queue("queued"))
        );
    }

    @Test
    void answersEveryCallerOfAFleet() {
        Assertions.assertEquals(
            List.of(5, 5, 5, 5, 5), StackIT.shared(StackIT.queue("fleet"))
        );
    }

    @Test
    void leavesTheResultWhereAnyoneCanFindIt() {
        final String queue = StackIT.queue("found");
        final String id;
        try (
            Broker broker = StackIT.store();
            Backend backend = new RedisBackend(StackIT.pool())
        ) {
            final Client client = new Client(
                broker, backend, new ProtocolV2(), new Routes().toward(queue)
            );
            final Handle handle = client.delay(StackIT.NAME, 2, 3);
            id = handle.id();
            StackIT.work(broker, backend, queue, 1);
            handle.value(StackIT.PATIENCE);
        }
        try (Backend other = new RedisBackend(StackIT.pool())) {
            Assertions.assertEquals(Optional.of(5), other.of(id).orElseThrow().value());
        }
    }

    private static Optional<Object> asked(final Broker broker, final String queue) {
        try (
            Broker carrying = broker;
            Backend backend = new RedisBackend(StackIT.pool())
        ) {
            final Handle handle = new Client(
                carrying, backend, new ProtocolV2(), new Routes().toward(queue)
            ).delay(StackIT.NAME, 2, 3);
            StackIT.work(carrying, backend, queue, 1);
            return handle.value(StackIT.PATIENCE);
        }
    }

    private static List<Object> shared(final String queue) {
        try (
            Broker broker = StackIT.store();
            Backend backend = new RedisBackend(StackIT.pool())
        ) {
            final Client client = new Client(
                broker, backend, new ProtocolV2(), new Routes().toward(queue)
            );
            final List<Handle> handles = List.of(
                client.delay(StackIT.NAME, 2, 3),
                client.delay(StackIT.NAME, 2, 3),
                client.delay(StackIT.NAME, 2, 3),
                client.delay(StackIT.NAME, 2, 3),
                client.delay(StackIT.NAME, 2, 3)
            );
            StackIT.work(broker, backend, queue, 5);
            return handles.stream()
                .map(handle -> handle.value(StackIT.PATIENCE).orElseThrow())
                .toList();
        }
    }

    private static void work(
        final Broker broker, final Backend backend, final String queue, final int count
    ) {
        try (
            Fleet fleet = new Fleet(
                new Worker(
                    broker,
                    backend,
                    new Registry(
                        Map.of(
                            StackIT.NAME,
                            task -> task.args().stream().mapToInt(arg -> (Integer) arg).sum()
                        )
                    )
                ),
                Executors.newFixedThreadPool(3),
                new AtomicBoolean()
            )
        ) {
            StackIT.stopAfter(fleet, Duration.ofMillis(200L * count + 1500L));
            fleet.run(List.of(queue), StackIT.WAIT, 3);
        }
    }

    private static void stopAfter(final Fleet fleet, final Duration delay) {
        final Thread clock = new Thread(
            () -> {
                try {
                    Thread.sleep(delay.toMillis());
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
                fleet.stop();
            }
        );
        clock.setDaemon(true);
        clock.start();
    }

    private static Broker store() {
        return new RedisBroker(StackIT.pool());
    }

    private static Broker queued() {
        return new AmqpBroker(StackIT.channel());
    }

    private static String queue(final String suffix) {
        return String.format("stack-%s-%s", suffix, UUID.randomUUID());
    }

    private static Channel channel() {
        final ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(StackIT.MESSAGES.getHost());
        factory.setPort(StackIT.MESSAGES.getAmqpPort());
        factory.setUsername(StackIT.MESSAGES.getAdminUsername());
        factory.setPassword(StackIT.MESSAGES.getAdminPassword());
        try {
            return factory.newConnection().createChannel();
        } catch (final IOException ex) {
            throw new TransportException("the queue service cannot be reached", ex);
        } catch (final TimeoutException ex) {
            throw new TransportException("the queue service took too long to answer", ex);
        }
    }

    private static JedisPool pool() {
        return new JedisPool(StackIT.STORE.getHost(), StackIT.STORE.getMappedPort(StackIT.PORT));
    }
}
