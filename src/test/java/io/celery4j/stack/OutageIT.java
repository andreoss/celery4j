/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2024
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.stack;

import io.celery4j.protocol.Message;
import io.celery4j.protocol.ProtocolV2;
import io.celery4j.protocol.Task;
import io.celery4j.transport.Broker;
import io.celery4j.transport.Reconnecting;
import io.celery4j.transport.RedisBroker;
import io.celery4j.transport.TransportException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Live test case for a service that goes away and comes back.
 *
 * <p>A broker that fails on command proves that the retrying works. It does
 * not prove that a real service, stopped mid-flight and started again, is
 * carried through. Here the service is paused, which leaves its address in
 * place and its answers absent, which is what an outage looks like from a
 * caller's side.</p>
 *
 * @since 0.3.0
 */
@Tag("live")
@Testcontainers
final class OutageIT {

    /**
     * Port the store listens on inside its container.
     */
    private static final int PORT = 6379;

    /**
     * How long a receive waits.
     */
    private static final Duration WAIT = Duration.ofSeconds(2L);

    /**
     * The store the cases run against.
     */
    @Container
    private static final GenericContainer<?> STORE =
        new GenericContainer<>("docker.io/library/redis:7-alpine")
            .withExposedPorts(OutageIT.PORT);

    @Test
    void carriesAMessageWhileTheServiceIsThere() {
        final String queue = OutageIT.queue();
        try (Broker broker = OutageIT.broker()) {
            broker.send(OutageIT.message(queue), queue);
            Assertions.assertTrue(broker.receive(queue, OutageIT.WAIT).isPresent());
        }
    }

    @Test
    void refusesWhileTheServiceIsAway() {
        final String queue = OutageIT.queue();
        try (Broker broker = OutageIT.broker()) {
            OutageIT.pause();
            try {
                Assertions.assertThrows(
                    TransportException.class, () -> broker.send(OutageIT.message(queue), queue)
                );
            } finally {
                OutageIT.resume();
            }
        }
    }

    @Test
    void triesForAWhileBeforeGivingUp() {
        final String queue = OutageIT.queue();
        try (Broker broker = OutageIT.broker()) {
            OutageIT.pause();
            final long started = System.nanoTime();
            try {
                Assertions.assertThrows(
                    TransportException.class, () -> broker.send(OutageIT.message(queue), queue)
                );
            } finally {
                OutageIT.resume();
            }
            Assertions.assertTrue(System.nanoTime() - started > Duration.ofMillis(300L).toNanos());
        }
    }

    @Test
    void carriesAgainOnceTheServiceIsBack() {
        final String queue = OutageIT.queue();
        try (Broker broker = OutageIT.broker()) {
            OutageIT.pause();
            OutageIT.resume();
            broker.send(OutageIT.message(queue), queue);
            Assertions.assertTrue(broker.receive(queue, OutageIT.WAIT).isPresent());
        }
    }

    @Test
    void keepsWhatItHeldWhileTheServiceWasAway() {
        final String queue = OutageIT.queue();
        try (Broker broker = OutageIT.broker()) {
            broker.send(OutageIT.message(queue), queue);
            OutageIT.pause();
            OutageIT.resume();
            Assertions.assertTrue(broker.receive(queue, OutageIT.WAIT).isPresent());
        }
    }

    private static void pause() {
        DockerClientFactory.instance()
            .client()
            .pauseContainerCmd(OutageIT.STORE.getContainerId())
            .exec();
    }

    private static void resume() {
        DockerClientFactory.instance()
            .client()
            .unpauseContainerCmd(OutageIT.STORE.getContainerId())
            .exec();
    }

    private static Broker broker() {
        return new Reconnecting(
            new RedisBroker(OutageIT.pool()), 3, Duration.ofMillis(200L)
        );
    }

    private static Message message(final String queue) {
        return new ProtocolV2().message(
            new Task(UUID.randomUUID().toString(), "proj.java.sum", List.of(2, 3), Map.of()),
            queue
        );
    }

    private static String queue() {
        return String.format("outage-%s", UUID.randomUUID());
    }

    private static JedisPool pool() {
        return new JedisPool(
            new JedisPoolConfig(),
            OutageIT.STORE.getHost(),
            OutageIT.STORE.getMappedPort(OutageIT.PORT),
            1000
        );
    }
}
