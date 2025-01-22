/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.result;

import java.util.Optional;

/**
 * The store a result is written to and read from, under the identifier of the
 * task it belongs to.
 *
 * @since 0.1.0
 */
public interface Backend extends AutoCloseable {

    /**
     * Write a result.
     *
     * @param result Result to write
     * @throws ResultException If the store cannot take it
     */
    void store(TaskResult result) throws ResultException;

    /**
     * Read the result of a task.
     *
     * @param id Identifier of the task
     * @return The result, empty when the store holds none
     * @throws ResultException If the store cannot be read
     */
    Optional<TaskResult> of(String id) throws ResultException;

    @Override
    void close() throws ResultException;
}
