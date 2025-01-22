/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.result;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link FakeBackend}, which also checks the store contract.
 *
 * @since 0.1.0
 */
final class FakeBackendTest implements BackendContract {

    @Override
    public Backend backend() {
        return new FakeBackend(new ConcurrentHashMap<>());
    }

    @Override
    public String id() {
        return "task";
    }

    @Test
    void forgetsEverythingWhenClosed() {
        final Backend backend = this.backend();
        backend.store(this.result("closed", State.SUCCESS));
        backend.close();
        Assertions.assertEquals(Optional.empty(), backend.of(this.id("closed")));
    }
}
