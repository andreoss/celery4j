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
 * Test case for value types that hand out nothing anybody can change.
 *
 * <p>A type here holds what it was given and hands back a view of it. Nothing
 * a caller does to what it received can reach the type, and asking for a type
 * with one more field leaves the one asked unmoved, which is what makes these
 * values rather than boxes.</p>
 *
 * @since 1.0.1
 */
final class UnchangeableTest {

    /**
     * Name of the task these cases use.
     */
    private static final String NAME = "proj.tasks.add";

    @Test
    void refusesToHaveItsHeadersChanged() {
        final Map<String, Object> headers =
            new MessageHeaders(Map.of(MessageHeaders.TASK, UnchangeableTest.NAME)).asMap();
        Assertions.assertThrows(
            UnsupportedOperationException.class, () -> headers.put(MessageHeaders.TASK, "no")
        );
    }

    @Test
    void keepsItsHeadersWhenOneMoreIsAskedFor() {
        final MessageHeaders headers =
            new MessageHeaders(Map.of(MessageHeaders.TASK, UnchangeableTest.NAME));
        headers.with(MessageHeaders.RETRIES, 2);
        Assertions.assertEquals(1, headers.asMap().size());
    }

    @Test
    void refusesToHaveItsPropertiesChanged() {
        final Map<String, Object> properties =
            new MessageProperties(Map.of(MessageProperties.TYPE, MessageProperties.JSON)).asMap();
        Assertions.assertThrows(
            UnsupportedOperationException.class, () -> properties.put("x", "y")
        );
    }

    @Test
    void keepsItsPropertiesWhenOneMoreIsAskedFor() {
        final MessageProperties properties =
            new MessageProperties(Map.of(MessageProperties.TYPE, MessageProperties.JSON));
        properties.with(MessageProperties.PRIORITY, 3);
        Assertions.assertEquals(1, properties.asMap().size());
    }

    @Test
    void refusesToHaveItsArgumentsChanged() {
        final List<Object> args =
            new Task("task-one", UnchangeableTest.NAME, List.of(2, 3), Map.of()).args();
        Assertions.assertThrows(UnsupportedOperationException.class, () -> args.add(4));
    }

    @Test
    void refusesToHaveItsKeywordArgumentsChanged() {
        final Map<String, Object> kwargs =
            new Task("task-one", UnchangeableTest.NAME, List.of(), Map.of("debug", true))
                .kwargs();
        Assertions.assertThrows(
            UnsupportedOperationException.class, () -> kwargs.put("debug", false)
        );
    }

    @Test
    void handsOutNoArgumentsWhenItWasGivenNone() {
        Assertions.assertEquals(
            List.of(), new Task("task-one", UnchangeableTest.NAME, null, null).args()
        );
    }

    @Test
    void refusesToHaveItsWorkFlowFieldsChanged() {
        final Map<String, Object> embed = new TaskBody(List.of(), Map.of(), Map.of()).embed();
        Assertions.assertThrows(
            UnsupportedOperationException.class, () -> embed.put(TaskBody.CHAIN, List.of())
        );
    }

    @Test
    void keepsItsBodyWhenOneMoreFieldIsAskedFor() {
        final TaskBody body = new TaskBody(List.of(), Map.of(), Map.of());
        body.with(TaskBody.CHAIN, List.of("something"));
        Assertions.assertEquals(Optional.empty(), body.embedded(TaskBody.CHAIN));
    }

    @Test
    void refusesToHaveTheCallChanged() {
        final Map<String, Object> fields =
            new Signature(Map.of(Signature.TASK, UnchangeableTest.NAME)).asMap();
        Assertions.assertThrows(
            UnsupportedOperationException.class, () -> fields.put(Signature.TASK, "no")
        );
    }

    @Test
    void refusesToHaveTheArgumentsOfACallChanged() {
        final List<Object> args = new Signature(
            Map.of(Signature.TASK, UnchangeableTest.NAME, Signature.ARGS, List.of(10))
        ).args();
        Assertions.assertThrows(UnsupportedOperationException.class, () -> args.add(11));
    }
}
