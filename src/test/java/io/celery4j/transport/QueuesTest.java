/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.transport;

import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link Queues}.
 *
 * @since 0.1.0
 */
final class QueuesTest {

    /**
     * Queue used across the cases.
     */
    private static final String QUEUE = "important";

    @Test
    void carriesTheLowestStepOnTheBaseName() {
        Assertions.assertEquals(QueuesTest.QUEUE, new Queues().named(QueuesTest.QUEUE, 0L));
    }

    @Test
    void namesAStepAfterTheSeparator() {
        Assertions.assertEquals(
            String.format("%s%s6", QueuesTest.QUEUE, Queues.SEPARATOR),
            new Queues().named(QueuesTest.QUEUE, 6L)
        );
    }

    @Test
    void roundsAPriorityDownToItsStep() {
        Assertions.assertEquals(
            new Queues().named(QueuesTest.QUEUE, 3L), new Queues().named(QueuesTest.QUEUE, 5L)
        );
    }

    @Test
    void carriesAPriorityBelowTheFirstStepOnTheBaseName() {
        Assertions.assertEquals(QueuesTest.QUEUE, new Queues().named(QueuesTest.QUEUE, 2L));
    }

    @Test
    void carriesAPriorityAboveTheLastStepOnTheLastQueue() {
        Assertions.assertEquals(
            new Queues().named(QueuesTest.QUEUE, 9L), new Queues().named(QueuesTest.QUEUE, 42L)
        );
    }

    @Test
    void namesEveryQueueHighestFirst() {
        Assertions.assertEquals(
            List.of(
                String.format("%s%s9", QueuesTest.QUEUE, Queues.SEPARATOR),
                String.format("%s%s6", QueuesTest.QUEUE, Queues.SEPARATOR),
                String.format("%s%s3", QueuesTest.QUEUE, Queues.SEPARATOR),
                QueuesTest.QUEUE
            ),
            new Queues().all(QueuesTest.QUEUE)
        );
    }

    @Test
    void takesTheStepsItWasGiven() {
        Assertions.assertEquals(
            List.of(QueuesTest.QUEUE), new Queues("-", List.of(0)).all(QueuesTest.QUEUE)
        );
    }

    @Test
    void takesTheSeparatorItWasGiven() {
        Assertions.assertEquals(
            "important-5", new Queues("-", List.of(0, 5)).named(QueuesTest.QUEUE, 7L)
        );
    }
}
