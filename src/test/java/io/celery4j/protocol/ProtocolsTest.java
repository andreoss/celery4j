/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link Protocols}.
 *
 * @since 0.1.0
 */
final class ProtocolsTest {

    /**
     * Task name used across the cases.
     */
    private static final String NAME = "proj.tasks.add";

    /**
     * Queue used across the cases.
     */
    private static final String QUEUE = "important";

    @Test
    void readsVersionTwoFromTheHeaders() {
        Assertions.assertInstanceOf(
            ProtocolV2.class,
            new Protocols().of(
                new ProtocolV2().message(ProtocolsTest.task(), ProtocolsTest.QUEUE)
            )
        );
    }

    @Test
    void fallsBackToVersionOne() {
        Assertions.assertInstanceOf(
            ProtocolV1.class,
            new Protocols().of(
                new ProtocolV1().message(ProtocolsTest.task(), ProtocolsTest.QUEUE)
            )
        );
    }

    @Test
    void readsATaskWrittenInVersionTwo() {
        Assertions.assertEquals(
            ProtocolsTest.NAME,
            new Protocols()
                .task(new ProtocolV2().message(ProtocolsTest.task(), ProtocolsTest.QUEUE))
                .name()
        );
    }

    @Test
    void readsATaskWrittenInVersionOne() {
        Assertions.assertEquals(
            ProtocolsTest.NAME,
            new Protocols()
                .task(new ProtocolV1().message(ProtocolsTest.task(), ProtocolsTest.QUEUE))
                .name()
        );
    }

    @Test
    void readsTheArgumentsOfATaskWrittenInVersionOne() {
        Assertions.assertEquals(
            List.of(2, 2),
            new Protocols()
                .task(new ProtocolV1().message(ProtocolsTest.task(), ProtocolsTest.QUEUE))
                .args()
        );
    }

    @Test
    void usesTheVersionsItWasGiven() {
        Assertions.assertInstanceOf(
            ProtocolV1.class,
            new Protocols(new ProtocolV1(), new ProtocolV1()).of(
                new ProtocolV2().message(ProtocolsTest.task(), ProtocolsTest.QUEUE)
            )
        );
    }

    private static Task task() {
        return new Task(
            "task-one", ProtocolsTest.NAME, List.of(2, 2), Map.of("debug", true)
        );
    }
}
