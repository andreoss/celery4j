/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link ProtocolException}.
 *
 * @since 0.1.0
 */
final class ProtocolExceptionTest {

    @Test
    void saysWhatWasRejected() {
        Assertions.assertEquals(
            "body is no base64 text",
            new ProtocolException("body is no base64 text").getMessage()
        );
    }

    @Test
    void keepsTheFailureItCameFrom() {
        final Throwable cause = new IllegalStateException("broken");
        Assertions.assertEquals(
            cause, new ProtocolException("message cannot be written", cause).getCause()
        );
    }
}
