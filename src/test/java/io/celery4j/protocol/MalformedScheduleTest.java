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
 * Test case for a schedule somebody wrote wrongly.
 *
 * <p>The time limit of a message is a pair of numbers, and a producer that
 * writes something else there is telling a worker how long a task may run in
 * a language it does not speak. Saying so is better than running the task
 * without a limit.</p>
 *
 * @since 1.0.1
 */
final class MalformedScheduleTest {

    @Test
    void refusesATimeLimitThatIsNoPair() {
        Assertions.assertThrows(
            ProtocolException.class,
            () -> MalformedScheduleTest.schedule("thirty seconds").limit()
        );
    }

    @Test
    void saysWhichHeaderIsNoPair() {
        Assertions.assertTrue(
            Assertions.assertThrows(
                ProtocolException.class,
                () -> MalformedScheduleTest.schedule("thirty seconds").limit()
            ).getMessage().contains(MessageHeaders.TIMELIMIT)
        );
    }

    @Test
    void refusesATimeLimitThatIsNoNumber() {
        Assertions.assertThrows(
            ProtocolException.class,
            () -> MalformedScheduleTest.schedule(List.of("soon")).limit()
        );
    }

    private static Schedule schedule(final Object limit) {
        return new Schedule(new MessageHeaders(Map.of(MessageHeaders.TIMELIMIT, limit)));
    }
}
