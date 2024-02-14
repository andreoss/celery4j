/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2024
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.result;

import io.celery4j.transport.FailingPool;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for what {@link RedisBackend} says when the store will not talk
 * to it.
 *
 * @since 1.0.1
 */
final class RedisBackendFailureTest {

    @Test
    void refusesToStoreWhenTheStoreWillNot() {
        Assertions.assertThrows(
            ResultException.class,
            () -> RedisBackendFailureTest.backend().store(RedisBackendFailureTest.result())
        );
    }

    @Test
    void refusesToReadWhenTheStoreWillNot() {
        Assertions.assertThrows(
            ResultException.class, () -> RedisBackendFailureTest.backend().of("task-one")
        );
    }

    @Test
    void saysWhichResultCouldNotBeRead() {
        Assertions.assertTrue(
            Assertions.assertThrows(
                ResultException.class, () -> RedisBackendFailureTest.backend().of("task-one")
            ).getMessage().contains("task-one")
        );
    }

    @Test
    void refusesToCloseWhenTheStoreWillNot() {
        Assertions.assertThrows(
            ResultException.class, () -> RedisBackendFailureTest.backend().close()
        );
    }

    private static Backend backend() {
        return new RedisBackend(new FailingPool());
    }

    private static TaskResult result() {
        return new TaskResult(
            Map.of(
                TaskResult.ID, "task-one",
                TaskResult.STATUS, State.SUCCESS,
                TaskResult.VALUE, 42,
                TaskResult.CHILDREN, List.of()
            )
        );
    }
}
