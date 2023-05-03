/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link ProtocolV2}.
 *
 * @since 0.1.0
 */
final class ProtocolV2Test {

    /**
     * Identifier used across the cases.
     */
    private static final String ID = "task-one";

    /**
     * Task name used across the cases.
     */
    private static final String NAME = "proj.tasks.add";

    /**
     * Queue used across the cases.
     */
    private static final String QUEUE = "important";

    /**
     * Node the protocol under test reports.
     */
    private static final String NODE = "one@node";

    @Test
    void namesTheTaskInTheHeaders() {
        Assertions.assertEquals(
            ProtocolV2Test.NAME, ProtocolV2Test.message().headers().task()
        );
    }

    @Test
    void namesTheIdentifierInTheHeaders() {
        Assertions.assertEquals(ProtocolV2Test.ID, ProtocolV2Test.message().headers().id());
    }

    @Test
    void namesItsLanguage() {
        Assertions.assertEquals(
            Optional.of(ProtocolV2.LANG),
            ProtocolV2Test.message().headers().text(MessageHeaders.LANG)
        );
    }

    @Test
    void makesTheTaskTheRootOfItsWorkflow() {
        Assertions.assertEquals(
            Optional.of(ProtocolV2Test.ID),
            ProtocolV2Test.message().headers().text(MessageHeaders.ROOT)
        );
    }

    @Test
    void namesTheNodeThatProducedIt() {
        Assertions.assertEquals(
            Optional.of(ProtocolV2Test.NODE),
            ProtocolV2Test.message().headers().text(MessageHeaders.ORIGIN)
        );
    }

    @Test
    void startsWithNoRetries() {
        Assertions.assertEquals(
            0L, ProtocolV2Test.message().headers().number(MessageHeaders.RETRIES, -1L)
        );
    }

    @Test
    void writesTheFieldsThatDoNotApply() {
        Assertions.assertTrue(
            ProtocolV2Test.message().headers().asMap().containsKey(MessageHeaders.PARENT)
        );
    }

    @Test
    void writesNoTimeLimits() {
        Assertions.assertEquals(
            Arrays.asList(null, null),
            ProtocolV2Test.message().headers().asMap().get(MessageHeaders.TIMELIMIT)
        );
    }

    @Test
    void routesToTheQueueItWasGiven() {
        Assertions.assertEquals(
            Map.of(
                MessageProperties.EXCHANGE, "", MessageProperties.ROUTING, ProtocolV2Test.QUEUE
            ),
            ProtocolV2Test.message().properties().asMap().get(MessageProperties.DELIVERY)
        );
    }

    @Test
    void correlatesTheResultWithTheTask() {
        Assertions.assertEquals(
            ProtocolV2Test.ID, ProtocolV2Test.message().properties().correlation()
        );
    }

    @Test
    void asksForItsOwnReplyQueue() {
        Assertions.assertEquals(
            Optional.of("id-one"),
            ProtocolV2Test.message().properties().text(MessageProperties.REPLY)
        );
    }

    @Test
    void tagsTheDelivery() {
        Assertions.assertEquals(
            Optional.of("id-two"),
            ProtocolV2Test.message().properties().text(MessageProperties.TAG)
        );
    }

    @Test
    void wrapsTheBodyAsTheTransportNeeds() {
        Assertions.assertEquals(
            Optional.of(MessageProperties.BASE64),
            ProtocolV2Test.message().properties().text(MessageProperties.WRAPPING)
        );
    }

    @Test
    void asksTheBrokerToPersistIt() {
        Assertions.assertEquals(
            MessageProperties.PERSISTENT,
            ProtocolV2Test.message().properties().number(MessageProperties.MODE, 0L)
        );
    }

    @Test
    void readsBackTheArgumentsItAskedFor() {
        Assertions.assertEquals(
            List.of(2, 2),
            ProtocolV2Test.protocol().task(ProtocolV2Test.message()).args()
        );
    }

    @Test
    void readsBackTheKeywordArgumentsItAskedFor() {
        Assertions.assertEquals(
            Map.of("debug", true),
            ProtocolV2Test.protocol().task(ProtocolV2Test.message()).kwargs()
        );
    }

    @Test
    void readsBackTheTaskName() {
        Assertions.assertEquals(
            ProtocolV2Test.NAME,
            ProtocolV2Test.protocol().task(ProtocolV2Test.message()).name()
        );
    }

    @Test
    void readsTheBodyOfAMessage() {
        Assertions.assertEquals(
            List.of(2, 2),
            ProtocolV2Test.protocol().body(ProtocolV2Test.message()).args()
        );
    }

    @Test
    void survivesTheEnvelopeItTravelsIn() {
        final MessageCodec codec = new JsonMessageCodec();
        Assertions.assertEquals(
            List.of(2, 2),
            ProtocolV2Test.protocol()
                .task(codec.decode(codec.encode(ProtocolV2Test.message())))
                .args()
        );
    }

    @Test
    void namesTheNodeItRunsOn() {
        Assertions.assertTrue(
            new ProtocolV2()
                .message(ProtocolV2Test.task(), ProtocolV2Test.QUEUE)
                .headers().text(MessageHeaders.ORIGIN).orElseThrow().contains("@")
        );
    }

    @Test
    void mintsItsOwnDeliveryIdentifiers() {
        Assertions.assertTrue(
            new ProtocolV2()
                .message(ProtocolV2Test.task(), ProtocolV2Test.QUEUE)
                .properties().text(MessageProperties.TAG).isPresent()
        );
    }

    @Test
    void namesAnUnknownHost() {
        Assertions.assertTrue(ProtocolV2.node(null).endsWith("@unknown"));
    }

    @Test
    void namesAnEmptyHost() {
        Assertions.assertTrue(ProtocolV2.node("").endsWith("@unknown"));
    }

    @Test
    void namesTheHostItWasGiven() {
        Assertions.assertTrue(ProtocolV2.node("box").endsWith("@box"));
    }

    @Test
    void schedulesAMessageToStartLater() {
        Assertions.assertEquals(
            Optional.of("2026-09-19T13:00:00Z"),
            new Schedule(ProtocolV2Test.message().headers())
                .at(Instant.parse("2026-09-19T13:00:00Z"))
                .text(MessageHeaders.ETA)
        );
    }

    @Test
    void limitsHowLongAMessageIsWorthRunning() {
        Assertions.assertEquals(
            Optional.of("2026-09-19T14:00:00Z"),
            new Schedule(ProtocolV2Test.message().headers())
                .until(Instant.parse("2026-09-19T14:00:00Z"))
                .text(MessageHeaders.EXPIRES)
        );
    }

    @Test
    void keepsTheOtherHeadersWhenScheduling() {
        Assertions.assertEquals(
            ProtocolV2Test.NAME,
            new Schedule(ProtocolV2Test.message().headers())
                .at(Instant.parse("2026-09-19T13:00:00Z"))
                .task()
        );
    }

    @Test
    void carriesEmbeddedWorkflowFieldsThroughACycle() {
        final TaskBodyCodec codec = new JsonTaskBody();
        final TaskBody body = new TaskBody(
            List.of(),
            Map.of(),
            Map.of(TaskBody.CHAIN, List.of("next"), TaskBody.CHORD, "after")
        );
        Assertions.assertEquals(body.embed(), codec.decode(codec.encode(body)).embed());
    }

    private static Message message() {
        return ProtocolV2Test.protocol().message(ProtocolV2Test.task(), ProtocolV2Test.QUEUE);
    }

    private static Task task() {
        return new Task(
            ProtocolV2Test.ID, ProtocolV2Test.NAME, List.of(2, 2), Map.of("debug", true)
        );
    }

    private static ProtocolV2 protocol() {
        final AtomicInteger counter = new AtomicInteger();
        final List<String> names = List.of("id-one", "id-two", "id-three", "id-four");
        return new ProtocolV2(
            new JsonTaskBody(),
            () -> names.get(counter.getAndIncrement()),
            ProtocolV2Test.NODE
        );
    }
}
