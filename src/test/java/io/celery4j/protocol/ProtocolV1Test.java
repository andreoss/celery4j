/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link ProtocolV1}.
 *
 * @since 0.1.0
 */
final class ProtocolV1Test {

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

    @Test
    void leavesTheHeadersEmpty() {
        Assertions.assertEquals(Map.of(), ProtocolV1Test.message().headers().asMap());
    }

    @Test
    void namesTheTaskInTheBody() {
        Assertions.assertEquals(
            ProtocolV1Test.NAME, new ProtocolV1().task(ProtocolV1Test.message()).name()
        );
    }

    @Test
    void identifiesTheTaskInTheBody() {
        Assertions.assertEquals(
            ProtocolV1Test.ID, new ProtocolV1().task(ProtocolV1Test.message()).id()
        );
    }

    @Test
    void readsBackTheArgumentsItAskedFor() {
        Assertions.assertEquals(
            List.of(2, 2), new ProtocolV1().task(ProtocolV1Test.message()).args()
        );
    }

    @Test
    void readsBackTheKeywordArgumentsItAskedFor() {
        Assertions.assertEquals(
            Map.of("debug", true), new ProtocolV1().task(ProtocolV1Test.message()).kwargs()
        );
    }

    @Test
    void startsWithNoRetries() {
        Assertions.assertEquals(0L, new ProtocolV1().retries(ProtocolV1Test.message()));
    }

    @Test
    void saysItsTimesAreCoordinated() throws IOException {
        Assertions.assertEquals(
            "true",
            String.valueOf(ProtocolV1Test.body(ProtocolV1Test.message()).get(ProtocolV1.UTC))
        );
    }

    @Test
    void routesToTheQueueItWasGiven() {
        Assertions.assertEquals(
            Map.of(
                MessageProperties.EXCHANGE, "", MessageProperties.ROUTING, ProtocolV1Test.QUEUE
            ),
            ProtocolV1Test.message().properties().asMap().get(MessageProperties.DELIVERY)
        );
    }

    @Test
    void correlatesTheResultWithTheTask() {
        Assertions.assertEquals(
            ProtocolV1Test.ID, ProtocolV1Test.message().properties().correlation()
        );
    }

    @Test
    void survivesTheEnvelopeItTravelsIn() {
        final MessageCodec codec = new JsonMessageCodec();
        Assertions.assertEquals(
            List.of(2, 2),
            new ProtocolV1()
                .task(codec.decode(codec.encode(ProtocolV1Test.message())))
                .args()
        );
    }

    @Test
    void readsABodyWithoutArguments() {
        Assertions.assertEquals(
            List.of(),
            new ProtocolV1().task(ProtocolV1Test.carrying("{\"task\":\"t\",\"id\":\"a\"}")).args()
        );
    }

    @Test
    void readsABodyWithoutKeywordArguments() {
        Assertions.assertEquals(
            Map.of(),
            new ProtocolV1()
                .task(ProtocolV1Test.carrying("{\"task\":\"t\",\"id\":\"a\"}"))
                .kwargs()
        );
    }

    @Test
    void refusesABodyWithoutTaskName() {
        final ProtocolV1 protocol = new ProtocolV1();
        final Message message = ProtocolV1Test.carrying("{\"id\":\"a\"}");
        Assertions.assertThrows(ProtocolException.class, () -> protocol.task(message));
    }

    @Test
    void refusesABodyWithoutIdentifier() {
        final ProtocolV1 protocol = new ProtocolV1();
        final Message message = ProtocolV1Test.carrying("{\"task\":\"t\"}");
        Assertions.assertThrows(ProtocolException.class, () -> protocol.task(message));
    }

    @Test
    void refusesABodyThatIsNoObject() {
        final ProtocolV1 protocol = new ProtocolV1();
        final Message message = ProtocolV1Test.carrying("[]");
        Assertions.assertThrows(ProtocolException.class, () -> protocol.task(message));
    }

    @Test
    void refusesABodyThatIsNoJson() {
        final ProtocolV1 protocol = new ProtocolV1();
        final Message message = ProtocolV1Test.carrying("{oops");
        Assertions.assertThrows(ProtocolException.class, () -> protocol.task(message));
    }

    @Test
    void refusesABodyThatIsNotWrapped() {
        final ProtocolV1 protocol = new ProtocolV1();
        final Message message = new Message(
            new MessageProperties(Map.of(MessageProperties.CORRELATION, ProtocolV1Test.ID)),
            new MessageHeaders(Map.of()),
            "not wrapped!"
        );
        Assertions.assertThrows(ProtocolException.class, () -> protocol.task(message));
    }

    @Test
    void refusesArgumentsOfAnotherKind() {
        final ProtocolV1 protocol = new ProtocolV1();
        final Message message = ProtocolV1Test.carrying("{\"task\":\"t\",\"id\":\"a\",\"args\":7}");
        Assertions.assertThrows(ProtocolException.class, () -> protocol.task(message));
    }

    @Test
    void refusesKeywordArgumentsOfAnotherKind() {
        final ProtocolV1 protocol = new ProtocolV1();
        final Message message =
            ProtocolV1Test.carrying("{\"task\":\"t\",\"id\":\"a\",\"kwargs\":7}");
        Assertions.assertThrows(ProtocolException.class, () -> protocol.task(message));
    }

    @Test
    void mintsItsOwnDeliveryIdentifiers() {
        Assertions.assertEquals(
            Optional.of("id-one"),
            ProtocolV1Test.message().properties().text(MessageProperties.REPLY)
        );
    }

    private static Message message() {
        final AtomicInteger counter = new AtomicInteger();
        final List<String> names = List.of("id-one", "id-two");
        return new ProtocolV1(
            new ObjectMapper(), () -> names.get(counter.getAndIncrement())
        ).message(
            new Task(
                ProtocolV1Test.ID, ProtocolV1Test.NAME, List.of(2, 2), Map.of("debug", true)
            ),
            ProtocolV1Test.QUEUE
        );
    }

    private static Message carrying(final String body) {
        return new Message(
            new MessageProperties(Map.of(MessageProperties.CORRELATION, ProtocolV1Test.ID)),
            new MessageHeaders(Map.of()),
            Base64.getEncoder().encodeToString(body.getBytes(StandardCharsets.UTF_8))
        );
    }

    private static Map<String, Object> body(final Message message) throws IOException {
        return new ObjectMapper().readValue(
            Base64.getDecoder().decode(message.body()),
            new TypeReference<Map<String, Object>>() { }
        );
    }
}
