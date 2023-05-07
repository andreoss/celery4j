/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.worker;

import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageHeaders;
import io.celery4j.protocol.MessageProperties;
import io.celery4j.protocol.ProtocolException;
import io.celery4j.protocol.ProtocolV2;
import io.celery4j.protocol.Protocols;
import io.celery4j.protocol.Task;
import io.celery4j.result.Backend;
import io.celery4j.result.FakeBackend;
import io.celery4j.result.State;
import io.celery4j.result.TaskResult;
import io.celery4j.transport.Broker;
import io.celery4j.transport.FakeBroker;
import io.celery4j.transport.Queues;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link Worker}.
 *
 * @since 0.1.0
 */
final class WorkerTest {

    /**
     * Task name used across the cases.
     */
    private static final String NAME = "proj.tasks.add";

    /**
     * Queue used across the cases.
     */
    private static final String QUEUE = "important";

    /**
     * Identifier used across the cases.
     */
    private static final String ID = "task-one";

    /**
     * How long the cases wait for a message.
     */
    private static final Duration WAIT = Duration.ofMillis(50L);

    @Test
    void reportsNothingWhenTheQueueIsEmpty() {
        Assertions.assertEquals(
            Optional.empty(),
            WorkerTest.worker(WorkerTest.broker(), WorkerTest.adding())
                .once(WorkerTest.QUEUE, WorkerTest.WAIT)
        );
    }

    @Test
    void runsARegisteredTask() {
        Assertions.assertEquals(
            Optional.of(4),
            WorkerTest.handled(WorkerTest.adding()).orElseThrow().value()
        );
    }

    @Test
    void marksARunTaskAsSucceeded() {
        Assertions.assertEquals(
            new State(State.SUCCESS),
            WorkerTest.handled(WorkerTest.adding()).orElseThrow().state()
        );
    }

    @Test
    void namesTheTaskInItsResult() {
        Assertions.assertEquals(
            WorkerTest.ID, WorkerTest.handled(WorkerTest.adding()).orElseThrow().id()
        );
    }

    @Test
    void stampsTheResultWithItsClock() {
        Assertions.assertEquals(
            "2026-09-19T12:00:00.000000",
            WorkerTest.handled(WorkerTest.adding())
                .orElseThrow()
                .asMap()
                .get(TaskResult.DONE)
        );
    }

    @Test
    void publishesWhatItRan() {
        final Broker broker = WorkerTest.broker();
        broker.send(WorkerTest.message(), WorkerTest.QUEUE);
        final Backend backend = new FakeBackend(new ConcurrentHashMap<>());
        WorkerTest.worker(broker, backend, WorkerTest.adding())
            .once(WorkerTest.QUEUE, WorkerTest.WAIT);
        Assertions.assertEquals(
            Optional.of(4), backend.of(WorkerTest.ID).orElseThrow().value()
        );
    }

    @Test
    void marksAFailedTaskAsFailed() {
        Assertions.assertEquals(
            new State(State.FAILURE),
            WorkerTest.handled(WorkerTest.raising()).orElseThrow().state()
        );
    }

    @Test
    void keepsWhatTheFailureSaid() {
        Assertions.assertEquals(
            List.of("boom"),
            ((Map<?, ?>) WorkerTest.handled(WorkerTest.raising())
                .orElseThrow()
                .value()
                .orElseThrow())
                .get(Worker.MESSAGE)
        );
    }

    @Test
    void keepsTheTypeThatWasRaised() {
        Assertions.assertEquals(
            "IllegalStateException",
            ((Map<?, ?>) WorkerTest.handled(WorkerTest.raising())
                .orElseThrow()
                .value()
                .orElseThrow())
                .get(Worker.TYPE)
        );
    }

    @Test
    void keepsTheTraceOfAFailure() {
        Assertions.assertTrue(
            WorkerTest.handled(WorkerTest.raising())
                .orElseThrow()
                .traceback()
                .orElseThrow()
                .contains("IllegalStateException")
        );
    }

    @Test
    void failsATaskNobodyRegistered() {
        Assertions.assertEquals(
            new State(State.FAILURE),
            WorkerTest.handled(new Registry(Map.of())).orElseThrow().state()
        );
    }

    @Test
    void saysWhichNameItDoesNotAnswerFor() {
        Assertions.assertTrue(
            WorkerTest.handled(new Registry(Map.of()))
                .orElseThrow()
                .traceback()
                .orElseThrow()
                .contains(WorkerTest.NAME)
        );
    }

    @Test
    void refusesAMessageThatIsNoTask() {
        final Broker broker = WorkerTest.broker();
        broker.send(WorkerTest.nonsense(), WorkerTest.QUEUE);
        final Worker worker = WorkerTest.worker(broker, WorkerTest.adding());
        Assertions.assertThrows(
            ProtocolException.class, () -> worker.once(WorkerTest.QUEUE, WorkerTest.WAIT)
        );
    }

    @Test
    void handlesAsManyAsItWasAskedFor() {
        final Broker broker = WorkerTest.broker();
        broker.send(WorkerTest.message(), WorkerTest.QUEUE);
        broker.send(WorkerTest.message(), WorkerTest.QUEUE);
        Assertions.assertEquals(
            2,
            WorkerTest.worker(broker, WorkerTest.adding())
                .some(WorkerTest.QUEUE, WorkerTest.WAIT, 5)
                .size()
        );
    }

    @Test
    void stopsWhenTheQueueRunsDry() {
        final Broker broker = WorkerTest.broker();
        broker.send(WorkerTest.message(), WorkerTest.QUEUE);
        Assertions.assertEquals(
            1,
            WorkerTest.worker(broker, WorkerTest.adding())
                .some(WorkerTest.QUEUE, WorkerTest.WAIT, 3)
                .size()
        );
    }

    private static Optional<TaskResult> handled(final Registry registry) {
        final Broker broker = WorkerTest.broker();
        broker.send(WorkerTest.message(), WorkerTest.QUEUE);
        return WorkerTest.worker(broker, registry).once(WorkerTest.QUEUE, WorkerTest.WAIT);
    }

    private static Worker worker(final Broker broker, final Registry registry) {
        return WorkerTest.worker(broker, new FakeBackend(new ConcurrentHashMap<>()), registry);
    }

    private static Worker worker(
        final Broker broker, final Backend backend, final Registry registry
    ) {
        return new Worker(
            broker,
            backend,
            registry,
            new Protocols(),
            Clock.fixed(Instant.parse("2026-09-19T12:00:00Z"), ZoneOffset.UTC)
        );
    }

    private static Broker broker() {
        return new FakeBroker(new ConcurrentHashMap<>(), new Queues());
    }

    private static Registry adding() {
        return new Registry(
            Map.of(
                WorkerTest.NAME,
                task -> task.args().stream().mapToInt(arg -> (Integer) arg).sum()
            )
        );
    }

    private static Registry raising() {
        return new Registry(
            Map.of(
                WorkerTest.NAME,
                task -> {
                    throw new IllegalStateException("boom");
                }
            )
        );
    }

    private static Message message() {
        return new ProtocolV2().message(
            new Task(WorkerTest.ID, WorkerTest.NAME, List.of(2, 2), Map.of()),
            WorkerTest.QUEUE
        );
    }

    private static Message nonsense() {
        return new Message(
            new MessageProperties(
                Map.of(
                    MessageProperties.CORRELATION, WorkerTest.ID,
                    MessageProperties.TYPE, MessageProperties.JSON
                )
            ),
            new MessageHeaders(
                Map.of(MessageHeaders.ID, WorkerTest.ID, MessageHeaders.TASK, WorkerTest.NAME)
            ),
            "not wrapped!"
        );
    }
}
