/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.worker;

import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageHeaders;
import io.celery4j.protocol.MessageProperties;
import io.celery4j.protocol.ProtocolException;
import io.celery4j.protocol.ProtocolV1;
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
import java.util.concurrent.atomic.AtomicInteger;
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

    @Test
    void sendsBackATaskThatIsNotDueYet() {
        final Broker broker = WorkerTest.broker();
        broker.send(
            WorkerTest.scheduled(MessageHeaders.ETA, "2026-09-19T13:00:00Z"), WorkerTest.QUEUE
        );
        Assertions.assertEquals(
            Optional.empty(),
            WorkerTest.worker(broker, WorkerTest.adding()).once(WorkerTest.QUEUE, WorkerTest.WAIT)
        );
    }

    @Test
    void keepsATaskThatIsNotDueYetOnTheQueue() {
        final Broker broker = WorkerTest.broker();
        broker.send(
            WorkerTest.scheduled(MessageHeaders.ETA, "2026-09-19T13:00:00Z"), WorkerTest.QUEUE
        );
        WorkerTest.worker(broker, WorkerTest.adding()).once(WorkerTest.QUEUE, WorkerTest.WAIT);
        Assertions.assertTrue(broker.receive(WorkerTest.QUEUE, WorkerTest.WAIT).isPresent());
    }

    @Test
    void runsATaskWhoseStartTimeHasPassed() {
        final Broker broker = WorkerTest.broker();
        broker.send(
            WorkerTest.scheduled(MessageHeaders.ETA, "2026-09-19T11:00:00Z"), WorkerTest.QUEUE
        );
        Assertions.assertEquals(
            new State(State.SUCCESS),
            WorkerTest.worker(broker, WorkerTest.adding())
                .once(WorkerTest.QUEUE, WorkerTest.WAIT)
                .orElseThrow()
                .state()
        );
    }

    @Test
    void callsOffATaskThatStoppedBeingWorthRunning() {
        final Broker broker = WorkerTest.broker();
        broker.send(
            WorkerTest.scheduled(MessageHeaders.EXPIRES, "2026-09-19T11:00:00Z"), WorkerTest.QUEUE
        );
        Assertions.assertEquals(
            new State(State.REVOKED),
            WorkerTest.worker(broker, WorkerTest.adding())
                .once(WorkerTest.QUEUE, WorkerTest.WAIT)
                .orElseThrow()
                .state()
        );
    }

    @Test
    void doesNotRunATaskThatStoppedBeingWorthRunning() {
        final AtomicInteger runs = new AtomicInteger();
        final Broker broker = WorkerTest.broker();
        broker.send(
            WorkerTest.scheduled(MessageHeaders.EXPIRES, "2026-09-19T11:00:00Z"), WorkerTest.QUEUE
        );
        WorkerTest.worker(
            broker,
            new Registry(Map.of(WorkerTest.NAME, task -> runs.incrementAndGet()))
        ).once(WorkerTest.QUEUE, WorkerTest.WAIT);
        Assertions.assertEquals(0, runs.get());
    }

    @Test
    void marksARetriedTaskAsWaiting() {
        Assertions.assertEquals(
            new State(State.RETRY),
            WorkerTest.handled(WorkerTest.retrying()).orElseThrow().state()
        );
    }

    @Test
    void sendsARetriedTaskBackToItsQueue() {
        final Broker broker = WorkerTest.broker();
        broker.send(WorkerTest.message(), WorkerTest.QUEUE);
        WorkerTest.worker(broker, WorkerTest.retrying()).once(WorkerTest.QUEUE, WorkerTest.WAIT);
        Assertions.assertTrue(broker.receive(WorkerTest.QUEUE, WorkerTest.WAIT).isPresent());
    }

    @Test
    void raisesTheRetryCountOfATaskItSendsBack() {
        final Broker broker = WorkerTest.broker();
        broker.send(WorkerTest.message(), WorkerTest.QUEUE);
        WorkerTest.worker(broker, WorkerTest.retrying()).once(WorkerTest.QUEUE, WorkerTest.WAIT);
        Assertions.assertEquals(
            1L,
            broker.receive(WorkerTest.QUEUE, WorkerTest.WAIT)
                .orElseThrow()
                .headers()
                .number(MessageHeaders.RETRIES, -1L)
        );
    }

    @Test
    void raisesTheRetryCountAgainOnASecondRetry() {
        final Broker broker = WorkerTest.broker();
        broker.send(WorkerTest.retried(1), WorkerTest.QUEUE);
        WorkerTest.worker(broker, WorkerTest.retrying())
            .once(WorkerTest.QUEUE, WorkerTest.WAIT);
        Assertions.assertEquals(
            2L,
            broker.receive(WorkerTest.QUEUE, WorkerTest.WAIT)
                .orElseThrow()
                .headers()
                .number(MessageHeaders.RETRIES, -1L)
        );
    }

    @Test
    void keepsWhatTheRetryAskedFor() {
        Assertions.assertTrue(
            WorkerTest.handled(WorkerTest.retrying())
                .orElseThrow()
                .traceback()
                .orElseThrow()
                .contains("RetryException")
        );
    }

    @Test
    void refusesToRetryATaskOfTheOlderProtocol() {
        final Broker broker = WorkerTest.broker();
        broker.send(
            new ProtocolV1().message(
                new Task(WorkerTest.ID, WorkerTest.NAME, List.of(2, 2), Map.of()),
                WorkerTest.QUEUE
            ),
            WorkerTest.QUEUE
        );
        final Worker worker = WorkerTest.worker(broker, WorkerTest.retrying());
        Assertions.assertThrows(
            WorkerException.class, () -> worker.once(WorkerTest.QUEUE, WorkerTest.WAIT)
        );
    }

    @Test
    void takesTheHighestPriorityFirst() {
        final Broker broker = WorkerTest.broker();
        broker.send(WorkerTest.message(), WorkerTest.QUEUE);
        broker.send(
            WorkerTest.prioritised("task-urgent", 9), WorkerTest.QUEUE
        );
        Assertions.assertEquals(
            "task-urgent",
            WorkerTest.worker(broker, WorkerTest.adding())
                .once(new Queues().all(WorkerTest.QUEUE), WorkerTest.WAIT)
                .orElseThrow()
                .id()
        );
    }

    @Test
    void readsEveryNameAQueueIsKnownBy() {
        final Broker broker = WorkerTest.broker();
        broker.send(WorkerTest.prioritised("task-urgent", 6), WorkerTest.QUEUE);
        Assertions.assertTrue(
            WorkerTest.worker(broker, WorkerTest.adding())
                .once(new Queues().all(WorkerTest.QUEUE), WorkerTest.WAIT)
                .isPresent()
        );
    }

    @Test
    void sendsARetryBackToTheQueueItWasRoutedTo() {
        final Broker broker = WorkerTest.broker();
        broker.send(WorkerTest.message(), WorkerTest.QUEUE);
        WorkerTest.worker(broker, WorkerTest.retrying())
            .once(new Queues().all(WorkerTest.QUEUE), WorkerTest.WAIT);
        Assertions.assertTrue(
            broker.receive(WorkerTest.QUEUE, WorkerTest.WAIT).isPresent()
        );
    }

    @Test
    void leavesNoResultWhenNoneIsWanted() {
        final Broker broker = WorkerTest.broker();
        broker.send(WorkerTest.ignoring(), WorkerTest.QUEUE);
        final Backend backend = new FakeBackend(new ConcurrentHashMap<>());
        WorkerTest.worker(broker, backend, WorkerTest.adding())
            .once(WorkerTest.QUEUE, WorkerTest.WAIT);
        Assertions.assertEquals(Optional.empty(), backend.of(WorkerTest.ID));
    }

    @Test
    void stillReportsAResultNobodyWanted() {
        final Broker broker = WorkerTest.broker();
        broker.send(WorkerTest.ignoring(), WorkerTest.QUEUE);
        Assertions.assertEquals(
            Optional.of(4),
            WorkerTest.worker(broker, WorkerTest.adding())
                .once(WorkerTest.QUEUE, WorkerTest.WAIT)
                .orElseThrow()
                .value()
        );
    }

    @Test
    void stopsRetryingWhenThePolicyIsSpent() {
        final Broker broker = WorkerTest.broker();
        broker.send(WorkerTest.retried(3), WorkerTest.QUEUE);
        Assertions.assertEquals(
            new State(State.FAILURE),
            WorkerTest.worker(broker, WorkerTest.retrying())
                .once(WorkerTest.QUEUE, WorkerTest.WAIT)
                .orElseThrow()
                .state()
        );
    }

    @Test
    void sendsNothingBackWhenThePolicyIsSpent() {
        final Broker broker = WorkerTest.broker();
        broker.send(WorkerTest.retried(3), WorkerTest.QUEUE);
        WorkerTest.worker(broker, WorkerTest.retrying())
            .once(WorkerTest.QUEUE, WorkerTest.WAIT);
        Assertions.assertEquals(
            Optional.empty(), broker.receive(WorkerTest.QUEUE, WorkerTest.WAIT)
        );
    }

    @Test
    void makesARetriedTaskWaitBeforeItsNextAttempt() {
        final Broker broker = WorkerTest.broker();
        broker.send(WorkerTest.message(), WorkerTest.QUEUE);
        WorkerTest.worker(broker, WorkerTest.retrying())
            .once(WorkerTest.QUEUE, WorkerTest.WAIT);
        Assertions.assertEquals(
            Optional.of("2026-09-19T12:00:01Z"),
            broker.receive(WorkerTest.QUEUE, WorkerTest.WAIT)
                .orElseThrow()
                .headers()
                .text(MessageHeaders.ETA)
        );
    }

    @Test
    void makesALaterRetryWaitLonger() {
        final Broker broker = WorkerTest.broker();
        broker.send(WorkerTest.retried(2), WorkerTest.QUEUE);
        WorkerTest.worker(broker, WorkerTest.retrying())
            .once(WorkerTest.QUEUE, WorkerTest.WAIT);
        Assertions.assertEquals(
            Optional.of("2026-09-19T12:00:04Z"),
            broker.receive(WorkerTest.QUEUE, WorkerTest.WAIT)
                .orElseThrow()
                .headers()
                .text(MessageHeaders.ETA)
        );
    }

    @Test
    void failsATaskThatOutranItsLimit() {
        final Broker broker = WorkerTest.broker();
        broker.send(WorkerTest.limited(), WorkerTest.QUEUE);
        Assertions.assertEquals(
            new State(State.FAILURE),
            WorkerTest.worker(broker, WorkerTest.slow())
                .once(WorkerTest.QUEUE, WorkerTest.WAIT)
                .orElseThrow()
                .state()
        );
    }

    @Test
    void saysWhyATaskThatOutranItsLimitFailed() {
        final Broker broker = WorkerTest.broker();
        broker.send(WorkerTest.limited(), WorkerTest.QUEUE);
        Assertions.assertEquals(
            "TimeoutException",
            ((Map<?, ?>) WorkerTest.worker(broker, WorkerTest.slow())
                .once(WorkerTest.QUEUE, WorkerTest.WAIT)
                .orElseThrow()
                .value()
                .orElseThrow())
                .get(Worker.TYPE)
        );
    }

    @Test
    void runsATaskThatKeepsWithinItsLimit() {
        final Broker broker = WorkerTest.broker();
        broker.send(WorkerTest.limited(), WorkerTest.QUEUE);
        Assertions.assertEquals(
            new State(State.SUCCESS),
            WorkerTest.worker(broker, WorkerTest.adding())
                .once(WorkerTest.QUEUE, WorkerTest.WAIT)
                .orElseThrow()
                .state()
        );
    }

    private static Message retried(final int retries) {
        final Message message = WorkerTest.message();
        return new Message(
            message.properties(),
            message.headers().with(MessageHeaders.RETRIES, retries),
            message.body()
        );
    }

    private static Message limited() {
        final Message message = WorkerTest.message();
        return new Message(
            message.properties(),
            message.headers().with(MessageHeaders.TIMELIMIT, List.of(1, 2)),
            message.body()
        );
    }

    private static Registry slow() {
        return new Registry(
            Map.of(
                WorkerTest.NAME,
                task -> {
                    try {
                        Thread.sleep(4000L);
                    } catch (final InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                    return 4;
                }
            )
        );
    }

    private static Message ignoring() {
        final Message message = WorkerTest.message();
        return new Message(
            message.properties(),
            message.headers().with(MessageHeaders.IGNORE, true),
            message.body()
        );
    }

    private static Message prioritised(final String id, final int priority) {
        final Message message = new ProtocolV2().message(
            new Task(id, WorkerTest.NAME, List.of(2, 2), Map.of()), WorkerTest.QUEUE
        );
        return new Message(
            message.properties().with(MessageProperties.PRIORITY, priority),
            message.headers(),
            message.body()
        );
    }

    private static Registry retrying() {
        return new Registry(
            Map.of(
                WorkerTest.NAME,
                task -> {
                    throw new RetryException("not yet");
                }
            )
        );
    }

    private static Message scheduled(final String header, final String time) {
        final Message message = WorkerTest.message();
        return new Message(
            message.properties(), message.headers().with(header, time), message.body()
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
            new Options(
                new Protocols(),
                Clock.fixed(Instant.parse("2026-09-19T12:00:00Z"), ZoneOffset.UTC),
                new RetryPolicy(),
                null
            )
        );
    }

    private static Broker broker() {
        return new FakeBroker(new ConcurrentHashMap<>(), new Queues(), new ConcurrentHashMap<>());
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
