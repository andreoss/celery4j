/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.interop;

import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageHeaders;
import io.celery4j.protocol.ProtocolV2;
import io.celery4j.protocol.Task;
import io.celery4j.result.Backend;
import io.celery4j.result.RedisBackend;
import io.celery4j.result.Results;
import io.celery4j.result.TaskResult;
import io.celery4j.transport.Broker;
import io.celery4j.transport.RedisBroker;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
 * Live test case for headers neither side was told about, crossing in both
 * directions.
 *
 * <p>The headers the protocol names are checked elsewhere. A deployment puts
 * its own there too, for tracing and for whatever it tracks, and a library
 * that drops or mangles what it does not recognise breaks that quietly. Both
 * directions are checked here because writing an unknown header and reading
 * one are different pieces of code.</p>
 *
 * @since 0.3.1
 */
@Tag("live")
@Testcontainers
final class ForeignHeadersIT {

    /**
     * Port the store listens on inside its container.
     */
    private static final int PORT = 6379;

    /**
     * Queue the foreign worker reads.
     */
    private static final String QUEUE = "interop";

    /**
     * Name of the task that returns the header it was asked about.
     */
    private static final String HEADERS = "proj.tasks.headers";

    /**
     * Header this library invents.
     */
    private static final String TRACE = "x-trace";

    /**
     * Header the foreign producer invents with a number in it.
     */
    private static final String DEPTH = "x-depth";

    /**
     * What this library puts in the header it invents.
     */
    private static final String MINE = "trace-of-this-library";

    /**
     * How long a receive waits.
     */
    private static final Duration WAIT = Duration.ofSeconds(10L);

    /**
     * How long a task may take to be run by the other side.
     */
    private static final Duration PATIENCE = Duration.ofSeconds(60L);

    /**
     * Network the store and the foreign side meet on.
     */
    private static final Network NETWORK = Network.newNetwork();

    /**
     * The store both sides use.
     */
    @Container
    private static final GenericContainer<?> STORE =
        new GenericContainer<>("docker.io/library/redis:7-alpine")
            .withExposedPorts(ForeignHeadersIT.PORT)
            .withNetwork(ForeignHeadersIT.NETWORK)
            .withNetworkAliases("store");

    /**
     * A worker of the original implementation.
     */
    @Container
    private static final GenericContainer<?> FOREIGN = ForeignHeadersIT.foreign();

    @Test
    void handsAnInventedHeaderToAForeignWorker() {
        Assertions.assertEquals(
            Optional.of(ForeignHeadersIT.MINE),
            ForeignHeadersIT.read(ForeignHeadersIT.TRACE).value()
        );
    }

    @Test
    void handsAnInventedNumberToAForeignWorker() {
        Assertions.assertEquals(
            Optional.of(11), ForeignHeadersIT.read(ForeignHeadersIT.DEPTH).value()
        );
    }

    @Test
    void readsAHeaderAForeignProducerInvented() {
        Assertions.assertEquals(
            Optional.of("trace-of-the-other-side"),
            ForeignHeadersIT.asked().headers().text(ForeignHeadersIT.TRACE)
        );
    }

    @Test
    void readsANumberAForeignProducerInvented() {
        Assertions.assertEquals(
            7L, ForeignHeadersIT.asked().headers().number(ForeignHeadersIT.DEPTH, 0L)
        );
    }

    @Test
    void readsAHeaderTheFrameworkInvented() {
        Assertions.assertEquals(
            Optional.of("[2, 3]"),
            ForeignHeadersIT.asked().headers().text(MessageHeaders.ARGSREPR)
        );
    }

    @Test
    void readsWhereAForeignProducerSaysItCameFrom() {
        Assertions.assertTrue(
            ForeignHeadersIT.asked().headers().text(MessageHeaders.ORIGIN).isPresent()
        );
    }

    @Test
    void keepsAForeignWorkerRunning() {
        Assertions.assertTrue(ForeignHeadersIT.FOREIGN.isRunning());
    }

    private static TaskResult read(final String name) {
        final String id = UUID.randomUUID().toString();
        try (Broker broker = new RedisBroker(ForeignHeadersIT.pool())) {
            broker.send(ForeignHeadersIT.invented(id, name), ForeignHeadersIT.QUEUE);
        }
        try (Backend backend = new RedisBackend(ForeignHeadersIT.pool())) {
            return new Results(backend, Duration.ofMillis(200L))
                .await(id, ForeignHeadersIT.PATIENCE)
                .orElseThrow();
        }
    }

    private static Message invented(final String id, final String name) {
        final Message message = new ProtocolV2().message(
            new Task(id, ForeignHeadersIT.HEADERS, List.of(name), Map.of()),
            ForeignHeadersIT.QUEUE
        );
        return new Message(
            message.properties(),
            message.headers()
                .with(ForeignHeadersIT.TRACE, ForeignHeadersIT.MINE)
                .with(ForeignHeadersIT.DEPTH, 11),
            message.body()
        );
    }

    private static Message asked() {
        final String queue = String.format("headers-%s", UUID.randomUUID());
        ForeignHeadersIT.ask(queue);
        try (Broker broker = new RedisBroker(ForeignHeadersIT.pool())) {
            return broker.receive(queue, ForeignHeadersIT.WAIT).orElseThrow();
        }
    }

    private static void ask(final String queue) {
        try {
            ForeignHeadersIT.FOREIGN.execInContainer(
                "python",
                "header.py",
                UUID.randomUUID().toString(),
                ForeignHeadersIT.HEADERS,
                queue
            );
        } catch (final IOException ex) {
            throw new IllegalStateException("the foreign producer could not be run", ex);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("running the foreign producer was interrupted", ex);
        }
    }

    private static GenericContainer<?> foreign() {
        return new GenericContainer<>(ForeignHeadersIT.image())
            .withNetwork(ForeignHeadersIT.NETWORK)
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
            ForeignHeadersIT.STORE.getHost(),
            ForeignHeadersIT.STORE.getMappedPort(ForeignHeadersIT.PORT)
        );
    }
}
