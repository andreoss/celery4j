/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2024
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.worker;

import io.celery4j.protocol.Task;

/**
 * What a worker runs when a task of a given name arrives.
 *
 * <p>What it returns becomes the value of the result. What it raises becomes
 * the failure of the result: a job reports trouble by raising, never by
 * returning a value that means trouble.</p>
 *
 * @since 0.1.0
 */
@FunctionalInterface
public interface Job {

    /**
     * Run a task.
     *
     * @param task Task to run, with the parameters it was called with
     * @return What the task returned, which may be null
     */
    Object run(Task task);
}
