/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.interop;

import io.celery4j.protocol.JsonMessageCodec;
import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageCodec;
import io.celery4j.protocol.MessageHeaders;
import io.celery4j.protocol.ProtocolV2;
import io.celery4j.protocol.TaskBody;
import io.celery4j.transport.Broker;
import io.celery4j.transport.RedisBroker;
import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import redis.clients.jedis.JedisPool;

/**
 * Live test case for the work-flow fields the other side writes.
 *
 * <p>This library does not run chains, groups or callbacks: it carries their
 * fields so that whoever does can. Carrying them has been checked against
 * messages this library wrote; here they are written by the framework that
 * defines them.</p>
 *
 * @since 0.3.0
 */
@Tag("live")
@Testcontainers
final class ForeignWorkflowIT {

    /**
     * Port the store listens on inside its container.
     */
    private static final int PORT = 6379;

    /**
     * How long a receive waits.
     */
    private static final Duration WAIT = Duration.ofSeconds(5L);

    /**
     * Network the store and the producer meet on.
     */
    private static final Network NETWORK = Network.newNetwork();

    /**
     * The store both sides use.
     */
    @Container
    private static final GenericContainer<?> STORE =
        new GenericContainer<>("docker.io/library/redis:7-alpine")
            .withExposedPorts(ForeignWorkflowIT.PORT)
            .withNetwork(ForeignWorkflowIT.NETWORK)
            .withNetworkAliases("store");

    /**
     * The foreign side, holding the producer these cases run.
     */
    @Container
    private static final GenericContainer<?> FOREIGN = ForeignWorkflowIT.foreign();

    @Test
    void carriesTheRestOfAChain() {
        Assertions.assertTrue(
            new ProtocolV2().body(ForeignWorkflowIT.written("chain"))
                .embedded(TaskBody.CHAIN)
                .isPresent()
        );
    }

    @Test
    void carriesTheGroupATaskBelongsTo() {
        Assertions.assertTrue(
            ForeignWorkflowIT.written("group").headers().text(MessageHeaders.GROUP).isPresent()
        );
    }

    @Test
    void carriesTheCallbacksOfATask() {
        Assertions.assertTrue(
            new ProtocolV2().body(ForeignWorkflowIT.written("link"))
                .embedded(TaskBody.CALLBACKS)
                .isPresent()
        );
    }

    @Test
    void keepsTheRestOfAChainThroughACycle() {
        final MessageCodec codec = new JsonMessageCodec();
        final Message message = ForeignWorkflowIT.written("chain");
        Assertions.assertEquals(
            new ProtocolV2().body(message).embed(),
            new ProtocolV2().body(codec.decode(codec.encode(message))).embed()
        );
    }

    @Test
    void keepsTheGroupThroughACycle() {
        final MessageCodec codec = new JsonMessageCodec();
        final Message message = ForeignWorkflowIT.written("group");
        Assertions.assertEquals(
            message.headers().asMap(),
            codec.decode(codec.encode(message)).headers().asMap()
        );
    }

    @Test
    void keepsTheCallbacksThroughACycle() {
        final MessageCodec codec = new JsonMessageCodec();
        final Message message = ForeignWorkflowIT.written("link");
        Assertions.assertEquals(
            new ProtocolV2().body(message).embedded(TaskBody.CALLBACKS),
            new ProtocolV2().body(codec.decode(codec.encode(message)))
                .embedded(TaskBody.CALLBACKS)
        );
    }

    @Test
    void keepsTheForeignSideRunning() {
        Assertions.assertTrue(ForeignWorkflowIT.FOREIGN.isRunning());
    }

    private static Message written(final String shape) {
        final String queue = String.format("workflow-%s", UUID.randomUUID());
        ForeignWorkflowIT.ask(shape, queue);
        try (Broker broker = new RedisBroker(ForeignWorkflowIT.pool())) {
            return broker.receive(queue, ForeignWorkflowIT.WAIT).orElseThrow();
        }
    }

    private static void ask(final String shape, final String queue) {
        try {
            ForeignWorkflowIT.FOREIGN.execInContainer("python", "workflow.py", shape, queue);
        } catch (final IOException ex) {
            throw new IllegalStateException("the foreign producer could not be run", ex);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("running the foreign producer was interrupted", ex);
        }
    }

    private static GenericContainer<?> foreign() {
        return new GenericContainer<>(ForeignWorkflowIT.image())
            .withNetwork(ForeignWorkflowIT.NETWORK)
            .withEnv("BROKER_URL", "redis://store:6379/0")
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

    private static JedisPool pool() {
        return new JedisPool(
            ForeignWorkflowIT.STORE.getHost(),
            ForeignWorkflowIT.STORE.getMappedPort(ForeignWorkflowIT.PORT)
        );
    }
}
