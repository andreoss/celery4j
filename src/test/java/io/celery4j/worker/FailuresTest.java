/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.worker;

import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for the wrapper a failure arrives in when the task ran somewhere
 * else.
 *
 * @since 1.0.1
 */
final class FailuresTest {

    @Test
    void handsBackWhatWasWrapped() {
        final Throwable raised = new IllegalStateException("boom");
        Assertions.assertEquals(
            raised, new Failures(new CompletionException(raised)).itself()
        );
    }

    @Test
    void handsBackAWrapperThatWrapsNothing() {
        final Throwable empty = new CompletionException(null);
        Assertions.assertEquals(empty, new Failures(empty).itself());
    }

    @Test
    void handsBackWhatWasNeverWrapped() {
        final Throwable raised = new IllegalStateException("boom");
        Assertions.assertEquals(raised, new Failures(raised).itself());
    }

    @Test
    void namesTheTypeThatWasRaised() {
        Assertions.assertEquals(
            "IllegalStateException",
            new Failures(new IllegalStateException("boom")).described().get(Worker.TYPE)
        );
    }
}
