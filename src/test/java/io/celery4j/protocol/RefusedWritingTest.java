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
 * Test case for a codec whose mapper will not write.
 *
 * @since 1.0.1
 */
final class RefusedWritingTest {

    @Test
    void refusesAnEnvelopeThatCannotBeWritten() {
        Assertions.assertThrows(
            ProtocolException.class,
            () -> new JsonMessageCodec(new RefusingMapper()).encode(RefusedWritingTest.message())
        );
    }

    @Test
    void refusesABodyThatCannotBeWritten() {
        Assertions.assertThrows(
            ProtocolException.class,
            () -> new JsonTaskBody(new RefusingMapper()).encode(
                new TaskBody(List.of(2, 3), Map.of(), Map.of())
            )
        );
    }

    @Test
    void refusesAnOlderBodyThatCannotBeWritten() {
        Assertions.assertThrows(
            ProtocolException.class,
            () -> new ProtocolV1(new RefusingMapper(), () -> "task-one").message(
                RefusedWritingTest.task(), "celery"
            )
        );
    }

    private static Message message() {
        return new ProtocolV2().message(RefusedWritingTest.task(), "celery");
    }

    private static Task task() {
        return new Task("task-one", "proj.tasks.add", List.of(2, 3), Map.of());
    }
}
