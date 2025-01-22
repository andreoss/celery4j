/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.worker;

import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageHeaders;
import io.celery4j.protocol.MessageProperties;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for what a worker reads about the delivery that carried a
 * message.
 *
 * @since 1.0.1
 */
final class RequestsTest {

    @Test
    void readsTheQueueTheDeliveryNames() {
        Assertions.assertEquals(
            "named",
            new Requests(RequestsTest.message(Map.of(MessageProperties.ROUTING, "named")))
                .queue(List.of("first"))
        );
    }

    @Test
    void takesTheFirstQueueWhenTheDeliverySaysNothing() {
        Assertions.assertEquals(
            "first", new Requests(RequestsTest.message(null)).queue(List.of("first"))
        );
    }

    @Test
    void takesTheFirstQueueWhenTheDeliveryNamesNoText() {
        Assertions.assertEquals(
            "first",
            new Requests(RequestsTest.message(Map.of(MessageProperties.ROUTING, 7)))
                .queue(List.of("first"))
        );
    }

    @Test
    void readsThatAResultIsWanted() {
        Assertions.assertFalse(new Requests(RequestsTest.message(null)).ignored());
    }

    private static Message message(final Map<String, Object> delivery) {
        final Map<String, Object> properties;
        if (delivery == null) {
            properties = Map.of(MessageProperties.TYPE, MessageProperties.JSON);
        } else {
            properties = Map.of(
                MessageProperties.TYPE, MessageProperties.JSON,
                MessageProperties.DELIVERY, delivery
            );
        }
        return new Message(
            new MessageProperties(properties),
            new MessageHeaders(Map.of(MessageHeaders.TASK, "proj.tasks.add")),
            "e30="
        );
    }
}
