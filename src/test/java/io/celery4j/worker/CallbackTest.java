/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.worker;

import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageHeaders;
import io.celery4j.protocol.ProtocolV2;
import io.celery4j.protocol.Signature;
import io.celery4j.protocol.Task;
import io.celery4j.protocol.TaskBody;
import io.celery4j.result.FakeBackend;
import io.celery4j.transport.Broker;
import io.celery4j.transport.FakeBroker;
import io.celery4j.transport.Queues;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for a worker that calls what a message names for success and for
 * failure.
 *
 * @since 1.0.0
 */
final class CallbackTest {

    /**
     * Name of the task these cases run.
     */
    private static final String NAME = "proj.tasks.add";

    /**
     * Queue the first task comes from.
     */
    private static final String QUEUE = "first";

    /**
     * Queue what follows goes to.
     */
    private static final String LATER = "second";

    /**
     * Identity of the first task.
     */
    private static final String ID = "task-one";

    /**
     * How long a receive waits.
     */
    private static final Duration WAIT = Duration.ofMillis(50L);

    @Test
    void callsWhatTheMessageNamesForSuccess() {
        Assertions.assertTrue(
            CallbackTest.after(TaskBody.CALLBACKS, CallbackTest.adding()).isPresent()
        );
    }

    @Test
    void putsTheResultInFrontOfWhatItCallsForSuccess() {
        Assertions.assertEquals(
            List.of(4, 10),
            new ProtocolV2()
                .task(CallbackTest.after(TaskBody.CALLBACKS, CallbackTest.adding()).orElseThrow())
                .args()
        );
    }

    @Test
    void callsEveryOneOfThemForSuccess() {
        final Broker broker = CallbackTest.ran(
            TaskBody.CALLBACKS,
            List.of(CallbackTest.call(), CallbackTest.call()),
            CallbackTest.adding()
        );
        broker.receive(CallbackTest.LATER, CallbackTest.WAIT);
        Assertions.assertTrue(broker.receive(CallbackTest.LATER, CallbackTest.WAIT).isPresent());
    }

    @Test
    void callsNothingForSuccessWhenTheTaskFailed() {
        Assertions.assertEquals(
            Optional.empty(), CallbackTest.after(TaskBody.CALLBACKS, CallbackTest.raising())
        );
    }

    @Test
    void callsWhatTheMessageNamesForFailure() {
        Assertions.assertTrue(
            CallbackTest.after(TaskBody.ERRBACKS, CallbackTest.raising()).isPresent()
        );
    }

    @Test
    void handsTheTaskThatFailedToWhatItCallsForFailure() {
        Assertions.assertEquals(
            List.of(CallbackTest.ID, 10),
            new ProtocolV2()
                .task(CallbackTest.after(TaskBody.ERRBACKS, CallbackTest.raising()).orElseThrow())
                .args()
        );
    }

    @Test
    void callsNothingForFailureWhenTheTaskSucceeded() {
        Assertions.assertEquals(
            Optional.empty(), CallbackTest.after(TaskBody.ERRBACKS, CallbackTest.adding())
        );
    }

    @Test
    void namesTheTaskBeforeAsTheParentOfWhatItCalls() {
        Assertions.assertEquals(
            Optional.of(CallbackTest.ID),
            CallbackTest.after(TaskBody.CALLBACKS, CallbackTest.adding())
                .orElseThrow()
                .headers()
                .text(MessageHeaders.PARENT)
        );
    }

    private static Optional<Message> after(final String field, final Registry registry) {
        return CallbackTest.ran(field, List.of(CallbackTest.call()), registry)
            .receive(CallbackTest.LATER, CallbackTest.WAIT);
    }

    private static Broker ran(
        final String field, final List<Object> calls, final Registry registry
    ) {
        final Broker broker = new FakeBroker(
            new ConcurrentHashMap<>(), new Queues(), new ConcurrentHashMap<>()
        );
        broker.send(CallbackTest.message(field, calls), CallbackTest.QUEUE);
        new Worker(broker, new FakeBackend(new ConcurrentHashMap<>()), registry)
            .once(CallbackTest.QUEUE, CallbackTest.WAIT);
        return broker;
    }

    private static Message message(final String field, final List<Object> calls) {
        final Task task = new Task(CallbackTest.ID, CallbackTest.NAME, List.of(2, 2), Map.of());
        return new ProtocolV2().message(
            task,
            new TaskBody(task.args(), task.kwargs(), Map.of(field, calls)),
            CallbackTest.QUEUE
        );
    }

    private static Map<String, Object> call() {
        return Map.of(
            Signature.TASK, CallbackTest.NAME,
            Signature.ARGS, List.of(10),
            Signature.KWARGS, Map.of(),
            Signature.OPTIONS, Map.of(Signature.QUEUE, CallbackTest.LATER)
        );
    }

    private static Registry adding() {
        return new Registry(
            Map.of(
                CallbackTest.NAME,
                task -> task.args().stream().mapToInt(arg -> (Integer) arg).sum()
            )
        );
    }

    private static Registry raising() {
        return new Registry(
            Map.of(
                CallbackTest.NAME,
                task -> {
                    throw new IllegalStateException("boom");
                }
            )
        );
    }
}
