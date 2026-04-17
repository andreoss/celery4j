/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.interop;

import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageHeaders;
import io.celery4j.protocol.ProtocolV2;
import io.celery4j.protocol.Protocols;
import io.celery4j.protocol.Task;
import io.celery4j.result.Backend;
import io.celery4j.result.RedisBackend;
import io.celery4j.result.Results;
import io.celery4j.result.State;
import io.celery4j.result.TaskResult;
import io.celery4j.transport.Broker;
import io.celery4j.transport.RedisBroker;
import io.celery4j.worker.Options;
import io.celery4j.worker.Registry;
import io.celery4j.worker.RetryException;
import io.celery4j.worker.RetryPolicy;
import io.celery4j.worker.Worker;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
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
 * Live test case for a message this library sent back for another attempt,
 * run by a worker of the original implementation.
 *
 * <p>Retrying is checked elsewhere between two workers of this library, which
 * proves only that this library agrees with itself. What a retried message is
 * worth depends on whether the other side takes it for the same task, under
 * the same identity and with one attempt spent, and that can only be seen by
 * handing it over.</p>
 *
 * <p>The foreign worker here starts only once the retry has been republished,
 * rather than being paused while this library works. Which of two live
 * consumers gets a message is nobody's promise, and a consumer that is frozen
 * with a read already in flight still takes one.</p>
 *
 * @since 1.0.0
 */
@Tag("live")
@Testcontainers
final class ForeignRetryIT {

    /**
     * Port the store listens on inside its container.
     */
    private static final int PORT = 6379;

    /**
     * Queue the handed-over attempt travels on.
     */
    private static final String QUEUE = String.format("retries-%s", UUID.randomUUID());

    /**
     * Identity of the task the other side is asked to finish.
     */
    private static final String HANDED = UUID.randomUUID().toString();

    /**
     * Name of the task the foreign side answers for.
     */
    private static final String ADD = "proj.tasks.add";

    /**
     * How long a receive waits.
     */
    private static final Duration WAIT = Duration.ofSeconds(3L);

    /**
     * How long a task may take to be run by the other side.
     */
    private static final Duration PATIENCE = Duration.ofSeconds(60L);

    /**
     * What the other side made of the handed-over attempt, read once.
     */
    private static final AtomicReference<TaskResult> DONE = new AtomicReference<>();

    /**
     * Network the store and the worker meet on.
     */
    private static final Network NETWORK = Network.newNetwork();

    /**
     * The store both sides use.
     */
    @Container
    private static final GenericContainer<?> STORE =
        new GenericContainer<>("docker.io/library/redis:7-alpine")
            .withExposedPorts(ForeignRetryIT.PORT)
            .withNetwork(ForeignRetryIT.NETWORK)
            .withNetworkAliases("store");

    /**
     * A worker of the original implementation, started by hand once this
     * library has republished what it is meant to run.
     */
    private static final GenericContainer<?> FOREIGN = ForeignRetryIT.foreign();

    @AfterAll
    static void stop() {
        if (ForeignRetryIT.FOREIGN.isRunning()) {
            ForeignRetryIT.FOREIGN.stop();
        }
    }

    @Test
    void runsARetryThisLibraryRepublished() {
        Assertions.assertEquals(Optional.of(4), ForeignRetryIT.handed().value());
    }

    @Test
    void marksARetryThisLibraryRepublishedAsSucceeded() {
        Assertions.assertEquals(new State(State.SUCCESS), ForeignRetryIT.handed().state());
    }

    @Test
    void keepsTheIdentityOfTheTaskItRepublished() {
        Assertions.assertEquals(ForeignRetryIT.HANDED, ForeignRetryIT.handed().id());
    }

    @Test
    void countsTheAttemptInTheMessageItRepublished() {
        Assertions.assertEquals(
            1L, ForeignRetryIT.republished().headers().number(MessageHeaders.RETRIES, 0L)
        );
    }

    @Test
    void keepsTheNameOfTheTaskItRepublished() {
        Assertions.assertEquals(
            ForeignRetryIT.ADD, new Protocols().task(ForeignRetryIT.republished()).name()
        );
    }

    @Test
    void keepsTheArgumentsOfTheTaskItRepublished() {
        Assertions.assertEquals(
            List.of(2, 2), new Protocols().task(ForeignRetryIT.republished()).args()
        );
    }

    @Test
    void startsAForeignWorkerThatKeepsRunning() {
        Assertions.assertTrue(ForeignRetryIT.started());
    }

    private static TaskResult handed() {
        if (ForeignRetryIT.DONE.get() == null) {
            ForeignRetryIT.DONE.set(ForeignRetryIT.hand());
        }
        return ForeignRetryIT.DONE.get();
    }

    private static TaskResult hand() {
        ForeignRetryIT.asked(ForeignRetryIT.HANDED, ForeignRetryIT.QUEUE);
        ForeignRetryIT.retried(ForeignRetryIT.QUEUE);
        if (!ForeignRetryIT.started()) {
            throw new IllegalStateException("the foreign worker did not start");
        }
        try (Backend backend = new RedisBackend(ForeignRetryIT.pool())) {
            return new Results(backend, Duration.ofMillis(200L))
                .await(ForeignRetryIT.HANDED, ForeignRetryIT.PATIENCE)
                .orElseThrow();
        }
    }

    private static Message republished() {
        final String queue = String.format("retry-%s", UUID.randomUUID());
        ForeignRetryIT.asked(UUID.randomUUID().toString(), queue);
        ForeignRetryIT.retried(queue);
        try (Broker broker = new RedisBroker(ForeignRetryIT.pool())) {
            return broker.receive(queue, ForeignRetryIT.WAIT).orElseThrow();
        }
    }

    private static void asked(final String id, final String queue) {
        try (Broker broker = new RedisBroker(ForeignRetryIT.pool())) {
            broker.send(
                new ProtocolV2().message(
                    new Task(id, ForeignRetryIT.ADD, List.of(2, 2), Map.of()), queue
                ),
                queue
            );
        }
    }

    private static void retried(final String queue) {
        try (
            Broker broker = new RedisBroker(ForeignRetryIT.pool());
            Backend backend = new RedisBackend(ForeignRetryIT.pool())
        ) {
            if (
                new Worker(
                    broker,
                    backend,
                    new Registry(
                        Map.of(
                            ForeignRetryIT.ADD,
                            task -> {
                                throw new RetryException("this one is for the other side");
                            }
                        )
                    ),
                    new Options(
                        new Protocols(),
                        Clock.systemUTC(),
                        new RetryPolicy(3, Duration.ofMillis(100L), Duration.ofSeconds(1L)),
                        null
                    )
                ).once(queue, ForeignRetryIT.WAIT).isEmpty()
            ) {
                throw new IllegalStateException("the worker took nothing to retry");
            }
        }
    }

    private static boolean started() {
        if (!ForeignRetryIT.FOREIGN.isRunning()) {
            ForeignRetryIT.FOREIGN.start();
        }
        return ForeignRetryIT.FOREIGN.isRunning();
    }

    private static GenericContainer<?> foreign() {
        return new GenericContainer<>(ForeignRetryIT.image())
            .withNetwork(ForeignRetryIT.NETWORK)
            .withEnv("BROKER_URL", "redis://store:6379/0")
            .withCommand(ForeignRetryIT.command())
            .withStartupTimeout(Duration.ofMinutes(5L));
    }

    private static String command() {
        return String.format(
            "celery -A tasks worker --loglevel=info --concurrency=1 -Q %s",
            ForeignRetryIT.QUEUE
        );
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
            ForeignRetryIT.STORE.getHost(),
            ForeignRetryIT.STORE.getMappedPort(ForeignRetryIT.PORT)
        );
    }
}
