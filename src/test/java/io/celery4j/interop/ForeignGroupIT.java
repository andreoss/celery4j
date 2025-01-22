/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.interop;

import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageHeaders;
import io.celery4j.result.FakeBackend;
import io.celery4j.result.State;
import io.celery4j.result.TaskResult;
import io.celery4j.transport.Broker;
import io.celery4j.transport.RedisBroker;
import io.celery4j.worker.Registry;
import io.celery4j.worker.Worker;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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
 * Live test case for a whole group the framework wrote.
 *
 * <p>A group is a handful of messages that belong together and have nothing to
 * say to each other, which is why the field naming the group is read here and
 * nothing else about it is. What had never been shown is the simple part: that
 * every task of one is taken, run and answered for, with the group they belong
 * to arriving intact on each.</p>
 *
 * @since 1.0.0
 */
@Tag("live")
@Testcontainers
final class ForeignGroupIT {

    /**
     * Port the store listens on inside its container.
     */
    private static final int PORT = 6379;

    /**
     * How long a receive waits.
     */
    private static final Duration WAIT = Duration.ofSeconds(10L);

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
            .withExposedPorts(ForeignGroupIT.PORT)
            .withNetwork(ForeignGroupIT.NETWORK)
            .withNetworkAliases("store");

    /**
     * The foreign side, holding the producer these cases run.
     */
    @Container
    private static final GenericContainer<?> FOREIGN = ForeignGroupIT.foreign();

    @Test
    void runsEveryTaskOfAGroupTheFrameworkSent() {
        Assertions.assertEquals(2, ForeignGroupIT.done().size());
    }

    @Test
    void answersForEveryTaskOfTheGroup() {
        Assertions.assertEquals(
            List.of(new State(State.SUCCESS), new State(State.SUCCESS)),
            ForeignGroupIT.done().stream().map(TaskResult::state).toList()
        );
    }

    @Test
    void keepsWhatEachTaskOfTheGroupWasGiven() {
        Assertions.assertEquals(
            List.of(Optional.of(List.of(1)), Optional.of(List.of(2))),
            ForeignGroupIT.done().stream().map(TaskResult::value).toList()
        );
    }

    @Test
    void readsOneGroupForEveryTaskOfIt() {
        final List<Message> messages = ForeignGroupIT.queued();
        Assertions.assertEquals(
            messages.get(0).headers().text(MessageHeaders.GROUP),
            messages.get(1).headers().text(MessageHeaders.GROUP)
        );
    }

    @Test
    void readsAGroupThatIsNamed() {
        Assertions.assertTrue(
            ForeignGroupIT.queued().get(0).headers().text(MessageHeaders.GROUP).isPresent()
        );
    }

    @Test
    void keepsTheForeignSideRunning() {
        Assertions.assertTrue(ForeignGroupIT.FOREIGN.isRunning());
    }

    private static List<TaskResult> done() {
        final String queue = ForeignGroupIT.queue();
        ForeignGroupIT.ask(queue);
        try (Broker broker = new RedisBroker(ForeignGroupIT.pool())) {
            return new Worker(
                broker,
                new FakeBackend(new ConcurrentHashMap<>()),
                new Registry(Map.of("proj.java.echo", task -> task.args()))
            ).some(queue, ForeignGroupIT.WAIT, 2);
        }
    }

    private static List<Message> queued() {
        final String queue = ForeignGroupIT.queue();
        ForeignGroupIT.ask(queue);
        final List<Message> messages = new ArrayList<>(2);
        try (Broker broker = new RedisBroker(ForeignGroupIT.pool())) {
            messages.add(broker.receive(queue, ForeignGroupIT.WAIT).orElseThrow());
            messages.add(broker.receive(queue, ForeignGroupIT.WAIT).orElseThrow());
        }
        return List.copyOf(messages);
    }

    private static void ask(final String queue) {
        try {
            ForeignGroupIT.FOREIGN.execInContainer("python", "workflow.py", "group", queue);
        } catch (final IOException ex) {
            throw new IllegalStateException("the foreign producer could not be run", ex);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("running the foreign producer was interrupted", ex);
        }
    }

    private static String queue() {
        return String.format("grouped-%s", UUID.randomUUID());
    }

    private static GenericContainer<?> foreign() {
        return new GenericContainer<>(ForeignGroupIT.image())
            .withNetwork(ForeignGroupIT.NETWORK)
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
            ForeignGroupIT.STORE.getHost(), ForeignGroupIT.STORE.getMappedPort(ForeignGroupIT.PORT)
        );
    }
}
