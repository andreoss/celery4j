/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.result;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Waiting for a task to finish.
 *
 * <p>A wait ends in one of three ways and never in a fourth: the task
 * finished, the task failed, or the time ran out. Nothing here blocks
 * indefinitely.</p>
 *
 * @since 0.1.0
 */
public final class Results {

    /**
     * How often the store is asked when no interval was given.
     */
    private static final Duration INTERVAL = Duration.ofMillis(50L);

    /**
     * Store the results are read from.
     */
    private final Backend backend;

    /**
     * How often the store is asked.
     */
    private final Duration interval;

    /**
     * Ctor.
     *
     * @param backend Store the results are read from
     */
    public Results(final Backend backend) {
        this(backend, Results.INTERVAL);
    }

    /**
     * Ctor.
     *
     * @param backend Store the results are read from
     * @param interval How often the store is asked
     */
    public Results(final Backend backend, final Duration interval) {
        this.backend = backend;
        this.interval = interval;
    }

    /**
     * Wait until a task reaches a state nothing follows.
     *
     * @param id Identifier of the task
     * @param timeout How long to wait
     * @return The result, empty when the time ran out first
     * @throws ResultException If the store cannot be read, or waiting was
     *  interrupted
     */
    public Optional<TaskResult> await(final String id, final Duration timeout)
        throws ResultException {
        final Instant deadline = Instant.now().plus(timeout);
        Optional<TaskResult> found = Optional.empty();
        while (true) {
            final Optional<TaskResult> stored = this.backend.of(id);
            if (stored.filter(result -> result.state().terminal()).isPresent()) {
                found = stored;
                break;
            }
            if (!Instant.now().isBefore(deadline)) {
                break;
            }
            this.pause();
        }
        return found;
    }

    /**
     * Wait for the value a task returned.
     *
     * @param id Identifier of the task
     * @param timeout How long to wait
     * @return The value, empty when the task returned none
     * @throws ResultException If the task failed, if the time ran out, or if
     *  the store cannot be read
     */
    public Optional<Object> value(final String id, final Duration timeout)
        throws ResultException {
        final TaskResult result = this.await(id, timeout).orElseThrow(
            () -> new ResultException(
                String.format("task %s did not finish within %s", id, timeout)
            )
        );
        if (result.state().failed()) {
            throw new ResultException(
                String.format(
                    "task %s failed: %s", id, result.traceback().orElse("no traceback")
                )
            );
        }
        return result.value();
    }

    private void pause() {
        try {
            Thread.sleep(this.interval.toMillis());
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ResultException("waiting was interrupted", ex);
        }
    }
}
