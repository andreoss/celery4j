/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link Message}.
 *
 * @since 0.1.0
 */
final class MessageTest {

    /**
     * Identifier used across the cases.
     */
    private static final String ID = "3802f860-8d3c-4dad-b18c-597fb2ac728b";

    /**
     * Task name used across the cases.
     */
    private static final String NAME = "proj.tasks.add";

    @Test
    void namesTheTaskIdentifierItCarries() {
        Assertions.assertEquals(MessageTest.ID, MessageTest.message().id());
    }

    @Test
    void namesTheTaskItCarries() {
        Assertions.assertEquals(MessageTest.NAME, MessageTest.message().task());
    }

    @Test
    void keepsTheBodyAsItTravels() {
        Assertions.assertEquals("e30=", MessageTest.message().body());
    }

    @Test
    void comparesEqualByValue() {
        Assertions.assertEquals(MessageTest.message(), MessageTest.message());
    }

    @Test
    void hashesByValue() {
        Assertions.assertEquals(
            MessageTest.message().hashCode(), MessageTest.message().hashCode()
        );
    }

    private static Message message() {
        return new Message(
            new MessageProperties(Map.of(MessageProperties.CORRELATION, MessageTest.ID)),
            new MessageHeaders(
                Map.of(MessageHeaders.ID, MessageTest.ID, MessageHeaders.TASK, MessageTest.NAME)
            ),
            "e30="
        );
    }
}
