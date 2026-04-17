/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.result;

import java.util.Map;
import java.util.Optional;

/**
 * A result store that keeps everything in memory, for cases that need a store
 * but not a service.
 *
 * @since 0.1.0
 */
public final class FakeBackend implements Backend {

    /**
     * Results by task identifier.
     */
    private final Map<String, TaskResult> held;

    /**
     * Ctor.
     *
     * @param held Results by task identifier
     */
    public FakeBackend(final Map<String, TaskResult> held) {
        this.held = held;
    }

    @Override
    public void store(final TaskResult result) {
        this.held.put(result.id(), result);
    }

    @Override
    public Optional<TaskResult> of(final String id) {
        return Optional.ofNullable(this.held.get(id));
    }

    @Override
    public void close() {
        this.held.clear();
    }
}
