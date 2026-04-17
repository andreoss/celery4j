/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.worker;

import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageHeaders;
import io.celery4j.protocol.ProtocolV1;
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
 * Test case for a worker that finishes what a chain asks for after the task
 * it was handed.
 *
 * @since 1.0.0
 */
final class ChainTest {

    /**
     * Name of the task these cases run.
     */
    private static final String NAME = "proj.tasks.add";

    /**
     * Queue the first task comes from.
     */
    private static final String QUEUE = "first";

    /**
     * Queue the next link goes to.
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
    void sendsTheNextLinkOfAChain() {
        Assertions.assertTrue(
            ChainTest.after(List.of(ChainTest.link(ChainTest.LATER, false))).isPresent()
        );
    }

    @Test
    void putsTheResultInFrontOfTheNextLink() {
        Assertions.assertEquals(List.of(4, 10), ChainTest.next(false).args());
    }

    @Test
    void leavesAnImmutableLinkAlone() {
        Assertions.assertEquals(List.of(10), ChainTest.next(true).args());
    }

    @Test
    void leavesNothingOfTheChainBehindTheLastLink() {
        Assertions.assertEquals(
            Optional.of(List.of()),
            ChainTest.rest(List.of(ChainTest.link(ChainTest.LATER, false)))
        );
    }

    @Test
    void carriesTheRestOfTheChainAlong() {
        Assertions.assertEquals(
            Optional.of(List.of(ChainTest.link(ChainTest.QUEUE, false))),
            ChainTest.rest(
                List.of(
                    ChainTest.link(ChainTest.QUEUE, false),
                    ChainTest.link(ChainTest.LATER, false)
                )
            )
        );
    }

    @Test
    void namesTheTaskBeforeAsTheParent() {
        Assertions.assertEquals(
            Optional.of(ChainTest.ID), ChainTest.header(MessageHeaders.PARENT)
        );
    }

    @Test
    void keepsTheTaskTheChainStartedWithAsItsRoot() {
        Assertions.assertEquals(
            Optional.of(ChainTest.ID), ChainTest.header(MessageHeaders.ROOT)
        );
    }

    @Test
    void sendsTheNextLinkWhereTheTaskBeforeCameFromWhenItNamesNoQueue() {
        Assertions.assertTrue(
            ChainTest.ran(List.of(ChainTest.link(null, false)))
                .receive(ChainTest.QUEUE, ChainTest.WAIT)
                .isPresent()
        );
    }

    @Test
    void sendsNothingWhenNothingFollows() {
        Assertions.assertEquals(
            Optional.empty(),
            ChainTest.ran(List.of()).receive(ChainTest.LATER, ChainTest.WAIT)
        );
    }

    @Test
    void takesTheTaskBeforeAsTheRootWhenTheMessageNamesNone() {
        final Broker broker = ChainTest.broker();
        final Message chained = ChainTest.chained(List.of(ChainTest.link(ChainTest.LATER, false)));
        broker.send(
            new Message(
                chained.properties(),
                chained.headers().with(MessageHeaders.ROOT, null),
                chained.body()
            ),
            ChainTest.QUEUE
        );
        ChainTest.worker(broker).once(ChainTest.QUEUE, ChainTest.WAIT);
        Assertions.assertEquals(
            Optional.of(ChainTest.ID),
            broker.receive(ChainTest.LATER, ChainTest.WAIT)
                .orElseThrow()
                .headers()
                .text(MessageHeaders.ROOT)
        );
    }

    @Test
    void sendsNothingForAMessageOfTheOlderVersion() {
        final Broker broker = ChainTest.broker();
        broker.send(
            new ProtocolV1().message(
                new Task(ChainTest.ID, ChainTest.NAME, List.of(2, 2), Map.of()), ChainTest.QUEUE
            ),
            ChainTest.QUEUE
        );
        ChainTest.worker(broker).once(ChainTest.QUEUE, ChainTest.WAIT);
        Assertions.assertEquals(
            Optional.empty(), broker.receive(ChainTest.LATER, ChainTest.WAIT)
        );
    }

    private static Task next(final boolean immutable) {
        return new ProtocolV2().task(
            ChainTest.after(List.of(ChainTest.link(ChainTest.LATER, immutable))).orElseThrow()
        );
    }

    private static Optional<Object> rest(final List<Object> chain) {
        return new ProtocolV2()
            .body(ChainTest.after(chain).orElseThrow())
            .embedded(TaskBody.CHAIN);
    }

    private static Optional<String> header(final String name) {
        return ChainTest.after(List.of(ChainTest.link(ChainTest.LATER, false)))
            .orElseThrow()
            .headers()
            .text(name);
    }

    private static Optional<Message> after(final List<Object> chain) {
        return ChainTest.ran(chain).receive(ChainTest.LATER, ChainTest.WAIT);
    }

    private static Broker ran(final List<Object> chain) {
        final Broker broker = ChainTest.broker();
        broker.send(ChainTest.chained(chain), ChainTest.QUEUE);
        ChainTest.worker(broker).once(ChainTest.QUEUE, ChainTest.WAIT);
        return broker;
    }

    private static Worker worker(final Broker broker) {
        return new Worker(
            broker,
            new FakeBackend(new ConcurrentHashMap<>()),
            new Registry(
                Map.of(
                    ChainTest.NAME,
                    task -> task.args().stream().mapToInt(arg -> (Integer) arg).sum()
                )
            )
        );
    }

    private static Message chained(final List<Object> chain) {
        final Task task = new Task(ChainTest.ID, ChainTest.NAME, List.of(2, 2), Map.of());
        return new ProtocolV2().message(
            task,
            new TaskBody(task.args(), task.kwargs(), Map.of(TaskBody.CHAIN, chain)),
            ChainTest.QUEUE
        );
    }

    private static Map<String, Object> link(final String queue, final boolean immutable) {
        final Map<String, Object> options;
        if (queue == null) {
            options = Map.of(Signature.ID, "task-two");
        } else {
            options = Map.of(Signature.ID, "task-two", Signature.QUEUE, queue);
        }
        return Map.of(
            Signature.TASK, ChainTest.NAME,
            Signature.ARGS, List.of(10),
            Signature.KWARGS, Map.of(),
            Signature.OPTIONS, options,
            Signature.IMMUTABLE, immutable
        );
    }

    private static Broker broker() {
        return new FakeBroker(new ConcurrentHashMap<>(), new Queues(), new ConcurrentHashMap<>());
    }
}
