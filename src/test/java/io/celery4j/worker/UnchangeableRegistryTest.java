/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.worker;

import io.celery4j.protocol.Task;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for a registry that answers for what it was given and nothing
 * else.
 *
 * @since 1.0.1
 */
final class UnchangeableRegistryTest {

    @Test
    void keepsWhatItAnswersForWhenOneMoreIsAskedFor() {
        final Registry registry = new Registry(Map.of("proj.tasks.add", task -> 4));
        registry.with("proj.tasks.other", task -> 5);
        Assertions.assertThrows(
            WorkerException.class, () -> registry.of("proj.tasks.other")
        );
    }

    @Test
    void answersWithWhatItWasGiven() {
        Assertions.assertEquals(
            4,
            new Registry(Map.of("proj.tasks.add", (Job) task -> 4))
                .of("proj.tasks.add")
                .run(new Task("task-one", "proj.tasks.add", List.of(), Map.of()))
        );
    }
}
