/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.transport;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.impl.LongStringHelper;
import io.celery4j.protocol.JsonTaskBody;
import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageHeaders;
import io.celery4j.protocol.MessageProperties;
import io.celery4j.protocol.ProtocolV2;
import io.celery4j.protocol.Task;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link AmqpMessages}.
 *
 * @since 0.2.0
 */
final class AmqpMessagesTest {

    /**
     * Queue used across the cases.
     */
    private static final String QUEUE = "important";

    @Test
    void writesTheContentTypeOfAMessage() {
        Assertions.assertEquals(
            MessageProperties.JSON,
            new AmqpMessages().properties(AmqpMessagesTest.message()).getContentType()
        );
    }

    @Test
    void writesTheCorrelationOfAMessage() {
        Assertions.assertEquals(
            "task-one",
            new AmqpMessages().properties(AmqpMessagesTest.message()).getCorrelationId()
        );
    }

    @Test
    void writesTheTaskHeadersOfAMessage() {
        Assertions.assertEquals(
            "proj.tasks.add",
            new AmqpMessages().properties(AmqpMessagesTest.message())
                .getHeaders()
                .get(MessageHeaders.TASK)
        );
    }

    @Test
    void asksForAPersistentDelivery() {
        Assertions.assertEquals(
            MessageProperties.PERSISTENT,
            new AmqpMessages().properties(AmqpMessagesTest.message()).getDeliveryMode()
        );
    }

    @Test
    void writesTheBodyWithoutItsWrapping() {
        Assertions.assertEquals(
            "[[2,2],{},{}]",
            new String(
                new AmqpMessages().body(AmqpMessagesTest.message()), StandardCharsets.UTF_8
            )
        );
    }

    @Test
    void readsTheTaskOfADelivery() {
        Assertions.assertEquals(
            "proj.tasks.add", AmqpMessagesTest.delivered(Map.of("task", "proj.tasks.add")).task()
        );
    }

    @Test
    void readsTheBodyOfADeliveryBackIntoItsWrapping() {
        Assertions.assertEquals(
            "W1syLDJdLHt9LHt9XQ==",
            AmqpMessagesTest.delivered(Map.of("task", "proj.tasks.add")).body()
        );
    }

    @Test
    void readsTextHeadersOfADelivery() {
        Assertions.assertEquals(
            Optional.of("py"),
            AmqpMessagesTest.delivered(
                Map.of("task", "t", "lang", LongStringHelper.asLongString("py"))
            ).headers().text(MessageHeaders.LANG)
        );
    }

    @Test
    void readsListHeadersOfADelivery() {
        Assertions.assertEquals(
            List.of("one"),
            AmqpMessagesTest.delivered(
                Map.of("task", "t", "chain", List.of(LongStringHelper.asLongString("one")))
            ).headers().asMap().get("chain")
        );
    }

    @Test
    void readsNumberHeadersOfADelivery() {
        Assertions.assertEquals(
            3L,
            AmqpMessagesTest.delivered(Map.of("task", "t", "retries", 3))
                .headers()
                .number(MessageHeaders.RETRIES, 0L)
        );
    }

    @Test
    void readsADeliveryWithoutHeaders() {
        Assertions.assertEquals(
            Map.of(),
            new AmqpMessages().message(
                new AMQP.BasicProperties.Builder().build(),
                "{}".getBytes(StandardCharsets.UTF_8),
                AmqpMessagesTest.QUEUE
            ).headers().asMap()
        );
    }

    @Test
    void namesTheQueueADeliveryCameFrom() {
        Assertions.assertEquals(
            Map.of(
                MessageProperties.EXCHANGE, "", MessageProperties.ROUTING, AmqpMessagesTest.QUEUE
            ),
            AmqpMessagesTest.delivered(Map.of("task", "t"))
                .properties()
                .asMap()
                .get(MessageProperties.DELIVERY)
        );
    }

    @Test
    void readsThePriorityOfADelivery() {
        Assertions.assertEquals(
            6L,
            new AmqpMessages().message(
                new AMQP.BasicProperties.Builder().priority(6).build(),
                "{}".getBytes(StandardCharsets.UTF_8),
                AmqpMessagesTest.QUEUE
            ).properties().number(MessageProperties.PRIORITY, 0L)
        );
    }

    @Test
    void readsTheDeliveryModeOfADelivery() {
        Assertions.assertEquals(
            MessageProperties.PERSISTENT,
            (int) new AmqpMessages().message(
                new AMQP.BasicProperties.Builder().deliveryMode(2).build(),
                "{}".getBytes(StandardCharsets.UTF_8),
                AmqpMessagesTest.QUEUE
            ).properties().number(MessageProperties.MODE, 0L)
        );
    }

    @Test
    void readsBackWhatItWrote() {
        final AmqpMessages messages = new AmqpMessages();
        final Message sent = AmqpMessagesTest.message();
        Assertions.assertEquals(
            sent.body(),
            messages.message(
                messages.properties(sent), messages.body(sent), AmqpMessagesTest.QUEUE
            ).body()
        );
    }

    @Test
    void readsBackTheHeadersItWrote() {
        final AmqpMessages messages = new AmqpMessages();
        final Message sent = AmqpMessagesTest.message();
        Assertions.assertEquals(
            sent.headers().asMap(),
            messages.message(
                messages.properties(sent), messages.body(sent), AmqpMessagesTest.QUEUE
            ).headers().asMap()
        );
    }

    private static Message delivered(final Map<String, Object> headers) {
        return new AmqpMessages().message(
            new AMQP.BasicProperties.Builder()
                .contentType(MessageProperties.JSON)
                .contentEncoding(MessageProperties.UTF8)
                .headers(headers)
                .build(),
            "[[2,2],{},{}]".getBytes(StandardCharsets.UTF_8),
            AmqpMessagesTest.QUEUE
        );
    }

    private static Message message() {
        return new ProtocolV2(new JsonTaskBody(), () -> "delivery-one", "1@node").message(
            new Task("task-one", "proj.tasks.add", Arrays.asList(2, 2), Map.of()),
            AmqpMessagesTest.QUEUE
        );
    }
}
