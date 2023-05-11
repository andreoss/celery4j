/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.client;

import io.celery4j.result.ResultException;
import io.celery4j.result.Results;
import io.celery4j.result.TaskResult;
import java.time.Duration;
import java.util.Optional;

/**
 * What a caller is left holding after asking for a task: its identifier, and
 * the means to find out what became of it.
 *
 * @since 0.2.0
 */
public final class Handle {

    /**
     * Identifier of the task.
     */
    private final String key;

    /**
     * Where the result is looked for.
     */
    private final Results results;

    /**
     * Ctor.
     *
     * @param key Identifier of the task
     * @param results Where the result is looked for
     */
    public Handle(final String key, final Results results) {
        this.key = key;
        this.results = results;
    }

    /**
     * Identifier of the task, which is how anyone else finds its result.
     *
     * @return The identifier
     */
    public String id() {
        return this.key;
    }

    /**
     * Wait until the task is finished.
     *
     * @param timeout How long to wait
     * @return The result, empty when the time ran out first
     * @throws ResultException If the store cannot be read
     */
    public Optional<TaskResult> result(final Duration timeout) throws ResultException {
        return this.results.await(this.key, timeout);
    }

    /**
     * Wait for what the task returned.
     *
     * @param timeout How long to wait
     * @return The value, empty when the task returned none
     * @throws ResultException If the task failed, if the time ran out, or if
     *  the store cannot be read
     */
    public Optional<Object> value(final Duration timeout) throws ResultException {
        return this.results.value(this.key, timeout);
    }

    /**
     * Whether the task is finished already.
     *
     * @return True when nothing more will happen to it
     * @throws ResultException If the store cannot be read
     */
    public boolean ready() throws ResultException {
        return this.results.await(this.key, Duration.ZERO).isPresent();
    }
}
