/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.transport;

import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageHeaders;
import io.celery4j.protocol.MessageProperties;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for a body the store wrapped and one it did not.
 *
 * <p>The store carries a body wrapped in base64 inside an envelope of its
 * own; this service carries bytes. A message that crossed from one to the
 * other says which of the two its body is, and a message written for this
 * service directly says nothing and means text.</p>
 *
 * @since 1.0.1
 */
final class AmqpBodyTest {

    @Test
    void unwrapsABodyThatSaysItIsWrapped() {
        Assertions.assertEquals(
            "[[2, 3], {}, {}]",
            new String(
                new AmqpMessages().body(AmqpBodyTest.message(MessageProperties.BASE64)),
                StandardCharsets.UTF_8
            )
        );
    }

    @Test
    void takesABodyThatSaysNothingAsText() {
        Assertions.assertEquals(
            "W1syLCAzXSwge30sIHt9XQ==",
            new String(
                new AmqpMessages().body(AmqpBodyTest.message(null)), StandardCharsets.UTF_8
            )
        );
    }

    @Test
    void writesABodyInTheEncodingTheMessageNames() {
        Assertions.assertEquals(
            5,
            new AmqpMessages().body(
                new Message(
                    new MessageProperties(
                        Map.of(
                            MessageProperties.TYPE, MessageProperties.JSON,
                            MessageProperties.ENCODING, "us-ascii"
                        )
                    ),
                    new MessageHeaders(Map.of(MessageHeaders.TASK, "proj.tasks.add")),
                    "hello"
                )
            ).length
        );
    }

    private static Message message(final String wrapping) {
        final Map<String, Object> properties;
        if (wrapping == null) {
            properties = Map.of(MessageProperties.TYPE, MessageProperties.JSON);
        } else {
            properties = Map.of(
                MessageProperties.TYPE, MessageProperties.JSON,
                MessageProperties.WRAPPING, wrapping
            );
        }
        return new Message(
            new MessageProperties(properties),
            new MessageHeaders(Map.of(MessageHeaders.TASK, "proj.tasks.add")),
            "W1syLCAzXSwge30sIHt9XQ=="
        );
    }
}
