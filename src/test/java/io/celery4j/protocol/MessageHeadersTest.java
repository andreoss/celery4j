/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link MessageHeaders}.
 *
 * @since 0.1.0
 */
final class MessageHeadersTest {

    /**
     * Identifier used across the cases.
     */
    private static final String ID = "0ad73c66-f4c9-4600-bd20-96746e720eed";

    /**
     * Task name used across the cases.
     */
    private static final String NAME = "proj.tasks.add";

    @Test
    void readsTheIdentifier() {
        Assertions.assertEquals(MessageHeadersTest.ID, MessageHeadersTest.headers().id());
    }

    @Test
    void readsTheTaskName() {
        Assertions.assertEquals(MessageHeadersTest.NAME, MessageHeadersTest.headers().task());
    }

    @Test
    void refusesHeadersWithoutIdentifier() {
        Assertions.assertThrows(
            ProtocolException.class,
            () -> new MessageHeaders(Map.of(MessageHeaders.TASK, MessageHeadersTest.NAME)).id()
        );
    }

    @Test
    void refusesHeadersWithoutTaskName() {
        Assertions.assertThrows(
            ProtocolException.class,
            () -> new MessageHeaders(Map.of(MessageHeaders.ID, MessageHeadersTest.ID)).task()
        );
    }

    @Test
    void readsATextHeader() {
        Assertions.assertEquals(
            Optional.of(ProtocolV2.LANG),
            MessageHeadersTest.headers().with(MessageHeaders.LANG, ProtocolV2.LANG)
                .text(MessageHeaders.LANG)
        );
    }

    @Test
    void readsANumberHeader() {
        Assertions.assertEquals(
            2L,
            MessageHeadersTest.headers().with(MessageHeaders.RETRIES, 2)
                .number(MessageHeaders.RETRIES, 0L)
        );
    }

    @Test
    void readsAnAbsentNumberHeaderAsTheOneGiven() {
        Assertions.assertEquals(
            0L, MessageHeadersTest.headers().number(MessageHeaders.RETRIES, 0L)
        );
    }

    @Test
    void carriesAHeaderItDoesNotKnow() {
        Assertions.assertEquals(
            Optional.of("deep"),
            MessageHeadersTest.headers().with("replaced_task_nesting", "deep")
                .text("replaced_task_nesting")
        );
    }

    @Test
    void keepsANullHeader() {
        Assertions.assertTrue(
            MessageHeadersTest.headers().with(MessageHeaders.PARENT, null)
                .asMap().containsKey(MessageHeaders.PARENT)
        );
    }

    @Test
    void leavesTheHeadersItCameFromAlone() {
        final MessageHeaders headers = MessageHeadersTest.headers();
        headers.with(MessageHeaders.GROUP, "group-one");
        Assertions.assertFalse(headers.asMap().containsKey(MessageHeaders.GROUP));
    }

    @Test
    void refusesToBeModifiedThroughItsMap() {
        final Map<String, Object> values = MessageHeadersTest.headers().asMap();
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> values.put(MessageHeaders.LANG, ProtocolV2.LANG)
        );
    }

    @Test
    void comparesEqualByValue() {
        Assertions.assertEquals(MessageHeadersTest.headers(), MessageHeadersTest.headers());
    }

    @Test
    void comparesEqualToItself() {
        final MessageHeaders headers = MessageHeadersTest.headers();
        Assertions.assertEquals(headers, headers);
    }

    @Test
    void comparesUnequalToOtherHeaders() {
        Assertions.assertNotEquals(
            MessageHeadersTest.headers(),
            MessageHeadersTest.headers().with(MessageHeaders.LANG, "py")
        );
    }

    @Test
    void comparesUnequalToAnotherKind() {
        Assertions.assertNotEquals(MessageHeadersTest.headers(), "not headers");
    }

    @Test
    void hashesByValue() {
        Assertions.assertEquals(
            MessageHeadersTest.headers().hashCode(), MessageHeadersTest.headers().hashCode()
        );
    }

    @Test
    void describesItself() {
        Assertions.assertTrue(
            MessageHeadersTest.headers().toString().contains(MessageHeadersTest.NAME)
        );
    }

    private static MessageHeaders headers() {
        final Map<String, Object> values = new HashMap<>();
        values.put(MessageHeaders.ID, MessageHeadersTest.ID);
        values.put(MessageHeaders.TASK, MessageHeadersTest.NAME);
        return new MessageHeaders(values);
    }
}
