/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link Schedule}.
 *
 * @since 0.1.0
 */
final class ScheduleTest {

    /**
     * A time the cases judge against.
     */
    private static final Instant NOW = Instant.parse("2026-09-19T12:00:00Z");

    @Test
    void readsAStartTimeWithAZone() {
        Assertions.assertEquals(
            Optional.of(Instant.parse("2026-09-19T11:00:00Z")),
            ScheduleTest.schedule(MessageHeaders.ETA, "2026-09-19T11:00:00+00:00").eta()
        );
    }

    @Test
    void readsAStartTimeWithoutAZoneAsCoordinated() {
        Assertions.assertEquals(
            Optional.of(Instant.parse("2026-09-19T11:00:00.527191Z")),
            ScheduleTest.schedule(MessageHeaders.ETA, "2026-09-19T11:00:00.527191").eta()
        );
    }

    @Test
    void readsAnAbsentStartTimeAsEmpty() {
        Assertions.assertEquals(
            Optional.empty(), new Schedule(new MessageHeaders(Map.of())).eta()
        );
    }

    @Test
    void readsAnExpiryTime() {
        Assertions.assertEquals(
            Optional.of(Instant.parse("2026-09-19T13:00:00Z")),
            ScheduleTest.schedule(MessageHeaders.EXPIRES, "2026-09-19T13:00:00Z").expires()
        );
    }

    @Test
    void refusesATimeThatIsNoDate() {
        Assertions.assertThrows(
            ProtocolException.class,
            () -> ScheduleTest.schedule(MessageHeaders.ETA, "tomorrow").eta()
        );
    }

    @Test
    void reportsATaskPastItsExpiry() {
        Assertions.assertTrue(
            ScheduleTest.schedule(MessageHeaders.EXPIRES, "2026-09-19T11:00:00Z")
                .expired(ScheduleTest.NOW)
        );
    }

    @Test
    void reportsATaskWithinItsExpiry() {
        Assertions.assertFalse(
            ScheduleTest.schedule(MessageHeaders.EXPIRES, "2026-09-19T13:00:00Z")
                .expired(ScheduleTest.NOW)
        );
    }

    @Test
    void reportsATaskWithoutExpiryAsLiving() {
        Assertions.assertFalse(
            new Schedule(new MessageHeaders(Map.of())).expired(ScheduleTest.NOW)
        );
    }

    @Test
    void reportsATaskExpiringAtThatMomentAsExpired() {
        Assertions.assertTrue(
            ScheduleTest.schedule(MessageHeaders.EXPIRES, "2026-09-19T12:00:00Z")
                .expired(ScheduleTest.NOW)
        );
    }

    @Test
    void reportsATaskWithoutStartTimeAsDue() {
        Assertions.assertTrue(new Schedule(new MessageHeaders(Map.of())).due(ScheduleTest.NOW));
    }

    @Test
    void reportsATaskWaitingForItsStartTime() {
        Assertions.assertFalse(
            ScheduleTest.schedule(MessageHeaders.ETA, "2026-09-19T13:00:00Z")
                .due(ScheduleTest.NOW)
        );
    }

    @Test
    void reportsATaskPastItsStartTimeAsDue() {
        Assertions.assertTrue(
            ScheduleTest.schedule(MessageHeaders.ETA, "2026-09-19T11:00:00Z")
                .due(ScheduleTest.NOW)
        );
    }

    private static Schedule schedule(final String name, final String time) {
        return new Schedule(new MessageHeaders(Map.of(name, time)));
    }
}
