/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.worker;

import io.celery4j.protocol.Task;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link Registry}.
 *
 * @since 0.1.0
 */
final class RegistryTest {

    /**
     * Task name used across the cases.
     */
    private static final String NAME = "proj.tasks.add";

    @Test
    void handsBackTheJobOfAName() {
        Assertions.assertEquals(
            4,
            RegistryTest.registry()
                .of(RegistryTest.NAME)
                .run(new Task("task-one", RegistryTest.NAME, List.of(), Map.of()))
        );
    }

    @Test
    void refusesANameNobodyRegistered() {
        Assertions.assertThrows(
            WorkerException.class, () -> RegistryTest.registry().of("proj.tasks.other")
        );
    }

    @Test
    void knowsTheNamesItAnswersFor() {
        Assertions.assertTrue(RegistryTest.registry().knows(RegistryTest.NAME));
    }

    @Test
    void doesNotKnowANameNobodyRegistered() {
        Assertions.assertFalse(RegistryTest.registry().knows("proj.tasks.other"));
    }

    @Test
    void learnsAnotherName() {
        Assertions.assertTrue(
            new Registry(Map.of()).with(RegistryTest.NAME, task -> 4).knows(RegistryTest.NAME)
        );
    }

    @Test
    void keepsTheNamesItKnewWhenLearning() {
        Assertions.assertTrue(
            RegistryTest.registry()
                .with("proj.tasks.mul", task -> 4)
                .knows(RegistryTest.NAME)
        );
    }

    @Test
    void leavesTheRegistryItCameFromAlone() {
        final Registry registry = RegistryTest.registry();
        registry.with("proj.tasks.mul", task -> 4);
        Assertions.assertFalse(registry.knows("proj.tasks.mul"));
    }

    @Test
    void namesEverythingItAnswersFor() {
        Assertions.assertEquals(Set.of(RegistryTest.NAME), RegistryTest.registry().names());
    }

    private static Registry registry() {
        return new Registry(Map.of(RegistryTest.NAME, task -> 4));
    }
}
