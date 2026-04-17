/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for what the codecs say about a value that cannot be written.
 *
 * <p>Everything else handed to them here is a number, a text or a list of
 * those. A caller who puts an object of their own in a header or an argument
 * finds out at the boundary, which is the only place that can tell them.</p>
 *
 * @since 1.0.1
 */
final class EncodingFailureTest {

    @Test
    void refusesAMessageItCannotWrite() {
        Assertions.assertThrows(
            ProtocolException.class,
            () -> new JsonMessageCodec().encode(EncodingFailureTest.strange())
        );
    }

    @Test
    void saysAMessageCannotBeWritten() {
        Assertions.assertTrue(
            Assertions.assertThrows(
                ProtocolException.class,
                () -> new JsonMessageCodec().encode(EncodingFailureTest.strange())
            ).getMessage().contains("JSON")
        );
    }

    @Test
    void refusesABodyItCannotWrite() {
        Assertions.assertThrows(
            ProtocolException.class,
            () -> new JsonTaskBody().encode(
                new TaskBody(List.of(new Object()), Map.of(), Map.of())
            )
        );
    }

    private static Message strange() {
        return new Message(
            new MessageProperties(Map.of(MessageProperties.TYPE, MessageProperties.JSON)),
            new MessageHeaders(Map.of(MessageHeaders.TASK, new Object())),
            "e30="
        );
    }
}
