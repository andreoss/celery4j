/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.client;

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
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link Client}.
 *
 * @since 0.2.0
 */
final class ClientTest {

    /**
     * Task name used across the cases.
     */
    private static final String NAME = "proj.tasks.add";

    /**
     * How long the cases wait.
     */
    private static final Duration WAIT = Duration.ofMillis(100L);

    @Test
    void sendsATaskByName() {
        final Broker broker = ClientTest.broker();
        new Client(broker, ClientTest.backend()).delay(ClientTest.NAME, 2, 2);
        Assertions.assertEquals(
            ClientTest.NAME,
            broker.receive(Routes.DEFAULT, ClientTest.WAIT).orElseThrow().task()
        );
    }

    @Test
    void sendsThePositionalArgumentsItWasGiven() {
        final Broker broker = ClientTest.broker();
        new Client(broker, ClientTest.backend()).delay(ClientTest.NAME, 2, 2);
        Assertions.assertEquals(
            List.of(2, 2),
            new Protocols()
                .task(broker.receive(Routes.DEFAULT, ClientTest.WAIT).orElseThrow())
                .args()
        );
    }

    @Test
    void sendsTheKeywordArgumentsItWasGiven() {
        final Broker broker = ClientTest.broker();
        new Client(broker, ClientTest.backend()).delay(ClientTest.NAME, Map.of("debug", true));
        Assertions.assertEquals(
            Map.of("debug", true),
            new Protocols()
                .task(broker.receive(Routes.DEFAULT, ClientTest.WAIT).orElseThrow())
                .kwargs()
        );
    }

    @Test
    void handsBackAHandleOnTheTask() {
        Assertions.assertFalse(
            new Client(ClientTest.broker(), ClientTest.backend())
                .delay(ClientTest.NAME, 2, 2)
                .id()
                .isEmpty()
        );
    }

    @Test
    void mintsAFreshIdentifierEachTime() {
        final Client client = new Client(ClientTest.broker(), ClientTest.backend());
        Assertions.assertNotEquals(
            client.delay(ClientTest.NAME, 2, 2).id(), client.delay(ClientTest.NAME, 2, 2).id()
        );
    }

    @Test
    void sendsAPreparedTask() {
        final Broker broker = ClientTest.broker();
        new Client(broker, ClientTest.backend())
            .call(new Task("task-one", ClientTest.NAME, List.of(1), Map.of()));
        Assertions.assertEquals(
            "task-one", broker.receive(Routes.DEFAULT, ClientTest.WAIT).orElseThrow().id()
        );
    }

    @Test
    void routesByTheRulesItWasGiven() {
        final Broker broker = ClientTest.broker();
        new Client(
            broker,
            ClientTest.backend(),
            new ProtocolV2(),
            new Routes().with("proj.tasks.*", "important")
        ).delay(ClientTest.NAME, 2, 2);
        Assertions.assertTrue(broker.receive("important", ClientTest.WAIT).isPresent());
    }

    @Test
    void waitsForWhatATaskReturned() {
        final Backend backend = ClientTest.backend();
        final Client client = new Client(ClientTest.broker(), backend);
        final Handle handle = client.delay(ClientTest.NAME, 2, 2);
        backend.store(ClientTest.finished(handle.id()));
        Assertions.assertEquals(Optional.of(4), handle.value(ClientTest.WAIT));
    }

    @Test
    void waitsForTheResultOfATask() {
        final Backend backend = ClientTest.backend();
        final Client client = new Client(ClientTest.broker(), backend);
        final Handle handle = client.delay(ClientTest.NAME, 2, 2);
        backend.store(ClientTest.finished(handle.id()));
        Assertions.assertEquals(
            Optional.of(new State(State.SUCCESS)),
            handle.result(ClientTest.WAIT).map(TaskResult::state)
        );
    }

    @Test
    void reportsATaskThatIsNotFinishedYet() {
        Assertions.assertFalse(
            new Client(ClientTest.broker(), ClientTest.backend())
                .delay(ClientTest.NAME, 2, 2)
                .ready()
        );
    }

    @Test
    void reportsATaskThatIsFinished() {
        final Backend backend = ClientTest.backend();
        final Client client = new Client(ClientTest.broker(), backend);
        final Handle handle = client.delay(ClientTest.NAME, 2, 2);
        backend.store(ClientTest.finished(handle.id()));
        Assertions.assertTrue(handle.ready());
    }

    @Test
    void handsBackAHandleOnSomebodyElsesTask() {
        final Backend backend = ClientTest.backend();
        backend.store(ClientTest.finished("task-elsewhere"));
        Assertions.assertTrue(
            new Client(ClientTest.broker(), backend).handle("task-elsewhere").ready()
        );
    }

    @Test
    void closesWhatItWasGiven() {
        final Broker broker = ClientTest.broker();
        final Client client = new Client(broker, ClientTest.backend());
        client.delay(ClientTest.NAME, 2, 2);
        client.close();
        Assertions.assertEquals(
            Optional.empty(), broker.receive(Routes.DEFAULT, ClientTest.WAIT)
        );
    }

    private static TaskResult finished(final String id) {
        return new TaskResult(
            Map.of(
                TaskResult.ID, id,
                TaskResult.STATUS, State.SUCCESS,
                TaskResult.VALUE, 4
            )
        );
    }

    private static Broker broker() {
        return new FakeBroker(new ConcurrentHashMap<>(), new Queues(), new ConcurrentHashMap<>());
    }

    private static Backend backend() {
        return new FakeBackend(new ConcurrentHashMap<>());
    }
}
