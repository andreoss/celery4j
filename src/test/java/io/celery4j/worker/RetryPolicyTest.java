/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.worker;

import java.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link RetryPolicy}.
 *
 * @since 0.2.0
 */
final class RetryPolicyTest {

    @Test
    void allowsAFirstAttempt() {
        Assertions.assertTrue(RetryPolicyTest.policy().allows(0L));
    }

    @Test
    void allowsAttemptsUpToItsLimit() {
        Assertions.assertTrue(RetryPolicyTest.policy().allows(2L));
    }

    @Test
    void refusesAttemptsPastItsLimit() {
        Assertions.assertFalse(RetryPolicyTest.policy().allows(3L));
    }

    @Test
    void refusesAttemptsWellPastItsLimit() {
        Assertions.assertFalse(RetryPolicyTest.policy().allows(30L));
    }

    @Test
    void waitsTheFirstDelayFirst() {
        Assertions.assertEquals(
            Duration.ofSeconds(1L), RetryPolicyTest.policy().delay(0L)
        );
    }

    @Test
    void waitsLongerTheSecondTime() {
        Assertions.assertEquals(
            Duration.ofSeconds(2L), RetryPolicyTest.policy().delay(1L)
        );
    }

    @Test
    void waitsLongerAgainTheThirdTime() {
        Assertions.assertEquals(
            Duration.ofSeconds(4L), RetryPolicyTest.policy().delay(2L)
        );
    }

    @Test
    void stopsGrowingAtTheLongestWait() {
        Assertions.assertEquals(
            Duration.ofSeconds(5L), RetryPolicyTest.policy().delay(10L)
        );
    }

    @Test
    void stopsGrowingAtAnAbsurdNumberOfRetries() {
        Assertions.assertEquals(
            Duration.ofSeconds(5L), RetryPolicyTest.policy().delay(1000L)
        );
    }

    @Test
    void hasDefaultsOfItsOwn() {
        Assertions.assertTrue(new RetryPolicy().allows(0L));
    }

    @Test
    void hasADefaultLimit() {
        Assertions.assertFalse(new RetryPolicy().allows(3L));
    }

    @Test
    void hasADefaultFirstWait() {
        Assertions.assertEquals(Duration.ofSeconds(1L), new RetryPolicy().delay(0L));
    }

    private static RetryPolicy policy() {
        return new RetryPolicy(3, Duration.ofSeconds(1L), Duration.ofSeconds(5L));
    }
}
