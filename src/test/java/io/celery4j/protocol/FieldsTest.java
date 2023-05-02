/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link Fields}.
 *
 * @since 0.1.0
 */
final class FieldsTest {

    @Test
    void readsText() {
        Assertions.assertEquals(
            Optional.of("py"), Fields.text(Map.of("lang", "py"), "lang")
        );
    }

    @Test
    void readsAbsentTextAsEmpty() {
        Assertions.assertEquals(Optional.empty(), Fields.text(Map.of(), "lang"));
    }

    @Test
    void readsEmptyTextAsEmpty() {
        Assertions.assertEquals(
            Optional.empty(), Fields.text(Map.of("lang", ""), "lang")
        );
    }

    @Test
    void readsNullTextAsEmpty() {
        final Map<String, Object> values = new HashMap<>();
        values.put("lang", null);
        Assertions.assertEquals(Optional.empty(), Fields.text(values, "lang"));
    }

    @Test
    void refusesTextOfAnotherKind() {
        final Map<String, Object> values = Map.of("lang", 7);
        Assertions.assertThrows(
            ProtocolException.class, () -> Fields.text(values, "lang")
        );
    }

    @Test
    void readsNumber() {
        Assertions.assertEquals(3L, Fields.number(Map.of("retries", 3), "retries", 0L));
    }

    @Test
    void readsAbsentNumberAsTheOneGiven() {
        Assertions.assertEquals(7L, Fields.number(Map.of(), "retries", 7L));
    }

    @Test
    void refusesNumberOfAnotherKind() {
        final Map<String, Object> values = Map.of("retries", "many");
        Assertions.assertThrows(
            ProtocolException.class, () -> Fields.number(values, "retries", 0L)
        );
    }
}
