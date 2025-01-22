/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.worker;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

/**
 * What a result says about a task that raised something.
 *
 * <p>The framework writes a failure as three fields and a trace, and reads
 * them back the same way, so a failure raised here has to look like one raised
 * there. What a worker catches is rarely what the task raised, because running
 * it somewhere else wraps it, and the wrapper is of no interest to anyone
 * reading the result.</p>
 *
 * @since 1.0.0
 */
final class Failures {

    /**
     * What was raised.
     */
    private final Throwable failure;

    /**
     * Ctor.
     *
     * @param failure What was raised, wrapper and all
     */
    Failures(final Throwable failure) {
        this.failure = failure;
    }

    /**
     * What was raised, without the wrapper running it somewhere else added.
     *
     * @return The failure itself
     */
    Throwable itself() {
        final Throwable found;
        if (this.failure instanceof CompletionException && this.failure.getCause() != null) {
            found = this.failure.getCause();
        } else {
            found = this.failure;
        }
        return found;
    }

    /**
     * The failure as the fields a result carries.
     *
     * @return Type, message and module of what was raised
     */
    Map<String, Object> described() {
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put(Worker.TYPE, this.failure.getClass().getSimpleName());
        values.put(Worker.MESSAGE, List.of(String.valueOf(this.failure.getMessage())));
        values.put(Worker.MODULE, this.failure.getClass().getPackageName());
        return values;
    }

    /**
     * The trace of the failure, as text.
     *
     * @return Every frame of it
     */
    String text() {
        final StringWriter written = new StringWriter();
        try (PrintWriter printer = new PrintWriter(written)) {
            this.failure.printStackTrace(printer);
        }
        return written.toString();
    }
}
