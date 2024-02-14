/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2024
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link TaskBody}.
 *
 * @since 0.1.0
 */
final class TaskBodyTest {

    @Test
    void readsAbsentPositionalArgumentsAsEmpty() {
        Assertions.assertEquals(List.of(), new TaskBody(null, null, null).args());
    }

    @Test
    void readsAbsentKeywordArgumentsAsEmpty() {
        Assertions.assertEquals(Map.of(), new TaskBody(null, null, null).kwargs());
    }

    @Test
    void readsAbsentEmbeddedFieldsAsEmpty() {
        Assertions.assertEquals(Map.of(), new TaskBody(null, null, null).embed());
    }

    @Test
    void readsItsPositionalArguments() {
        Assertions.assertEquals(List.of("fizz"), TaskBodyTest.body().args());
    }

    @Test
    void readsItsKeywordArguments() {
        Assertions.assertEquals(Map.of("b", "bazz"), TaskBodyTest.body().kwargs());
    }

    @Test
    void readsItsEmbeddedFields() {
        Assertions.assertEquals(
            Map.of(TaskBody.CHAIN, List.of()), TaskBodyTest.body().embed()
        );
    }

    @Test
    void refusesToHaveItsArgumentsModified() {
        final List<Object> args = TaskBodyTest.body().args();
        Assertions.assertThrows(UnsupportedOperationException.class, () -> args.add("more"));
    }

    @Test
    void refusesToHaveItsEmbeddedFieldsModified() {
        final Map<String, Object> embed = TaskBodyTest.body().embed();
        Assertions.assertThrows(
            UnsupportedOperationException.class, () -> embed.put(TaskBody.CHORD, "x")
        );
    }

    @Test
    void refusesToHaveItsKeywordArgumentsModified() {
        final Map<String, Object> kwargs = TaskBodyTest.body().kwargs();
        Assertions.assertThrows(
            UnsupportedOperationException.class, () -> kwargs.put("c", "x")
        );
    }

    @Test
    void comparesEqualByValue() {
        Assertions.assertEquals(TaskBodyTest.body(), TaskBodyTest.body());
    }

    @Test
    void readsAnEmbeddedField() {
        Assertions.assertEquals(
            Optional.of(List.of()), TaskBodyTest.body().embedded(TaskBody.CHAIN)
        );
    }

    @Test
    void readsAnAbsentEmbeddedFieldAsEmpty() {
        Assertions.assertEquals(
            Optional.empty(), TaskBodyTest.body().embedded(TaskBody.CHORD)
        );
    }

    @Test
    void readsAnEmbeddedFieldOfABodyWithoutAny() {
        Assertions.assertEquals(
            Optional.empty(), new TaskBody(null, null, null).embedded(TaskBody.CHAIN)
        );
    }

    @Test
    void addsAKeywordArgument() {
        Assertions.assertEquals(
            Map.of("b", "bazz", "c", "cuzz"),
            TaskBodyTest.body().with("c", "cuzz").kwargs()
        );
    }

    @Test
    void keepsItsArgumentsWhenAKeywordIsAdded() {
        Assertions.assertEquals(
            List.of("fizz"), TaskBodyTest.body().with("c", "cuzz").args()
        );
    }

    @Test
    void comparesEqualToItself() {
        final TaskBody body = TaskBodyTest.body();
        Assertions.assertEquals(body, body);
    }

    @Test
    void comparesUnequalToOtherArguments() {
        Assertions.assertNotEquals(
            TaskBodyTest.body(),
            new TaskBody(List.of("buzz"), Map.of("b", "bazz"), Map.of(TaskBody.CHAIN, List.of()))
        );
    }

    @Test
    void comparesUnequalToOtherKeywordArguments() {
        Assertions.assertNotEquals(
            TaskBodyTest.body(),
            new TaskBody(List.of("fizz"), Map.of(), Map.of(TaskBody.CHAIN, List.of()))
        );
    }

    @Test
    void comparesUnequalToOtherEmbeddedFields() {
        Assertions.assertNotEquals(
            TaskBodyTest.body(), new TaskBody(List.of("fizz"), Map.of("b", "bazz"), Map.of())
        );
    }

    @Test
    void comparesUnequalToAnotherKind() {
        Assertions.assertNotEquals(TaskBodyTest.body(), "not a body");
    }

    @Test
    void hashesByValue() {
        Assertions.assertEquals(
            TaskBodyTest.body().hashCode(), TaskBodyTest.body().hashCode()
        );
    }

    @Test
    void describesItself() {
        Assertions.assertTrue(TaskBodyTest.body().toString().contains("fizz"));
    }

    private static TaskBody body() {
        return new TaskBody(
            List.of("fizz"), Map.of("b", "bazz"), Map.of(TaskBody.CHAIN, List.of())
        );
    }
}
