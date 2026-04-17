/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link BodyCodecs}.
 *
 * @since 0.1.0
 */
final class BodyCodecsTest {

    /**
     * Identifier used across the cases.
     */
    private static final String ID = "task-one";

    @Test
    void knowsTheJsonMimeType() {
        Assertions.assertInstanceOf(
            JsonTaskBody.class, new BodyCodecs().of(MessageProperties.JSON)
        );
    }

    @Test
    void knowsTheShortNameOfJson() {
        Assertions.assertInstanceOf(JsonTaskBody.class, new BodyCodecs().of(BodyCodecs.JSON));
    }

    @Test
    void refusesAnUnregisteredType() {
        final BodyCodecs codecs = new BodyCodecs();
        Assertions.assertThrows(
            ProtocolException.class, () -> codecs.of("application/x-python-serialize")
        );
    }

    @Test
    void readsTheTypeOfAMessage() {
        Assertions.assertInstanceOf(
            JsonTaskBody.class, new BodyCodecs().of(BodyCodecsTest.message(MessageProperties.JSON))
        );
    }

    @Test
    void refusesAMessageWithoutContentType() {
        final BodyCodecs codecs = new BodyCodecs();
        final Message message = new Message(
            new MessageProperties(Map.of(MessageProperties.CORRELATION, BodyCodecsTest.ID)),
            new MessageHeaders(Map.of()),
            "e30="
        );
        Assertions.assertThrows(ProtocolException.class, () -> codecs.of(message));
    }

    @Test
    void refusesAMessageOfAnUnregisteredType() {
        final BodyCodecs codecs = new BodyCodecs();
        final Message message = BodyCodecsTest.message("application/x-msgpack");
        Assertions.assertThrows(ProtocolException.class, () -> codecs.of(message));
    }

    @Test
    void learnsAnotherType() {
        Assertions.assertInstanceOf(
            JsonTaskBody.class,
            new BodyCodecs(Map.of())
                .with("application/x-yaml", new JsonTaskBody())
                .of("application/x-yaml")
        );
    }

    @Test
    void keepsTheTypesItKnewWhenLearning() {
        Assertions.assertInstanceOf(
            JsonTaskBody.class,
            new BodyCodecs().with("application/x-yaml", new JsonTaskBody())
                .of(MessageProperties.JSON)
        );
    }

    private static Message message(final String type) {
        return new Message(
            new MessageProperties(
                Map.of(
                    MessageProperties.CORRELATION, BodyCodecsTest.ID,
                    MessageProperties.TYPE, type
                )
            ),
            new MessageHeaders(Map.of()),
            "e30="
        );
    }
}
