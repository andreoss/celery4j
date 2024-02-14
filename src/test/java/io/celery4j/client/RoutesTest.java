/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2024
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.client;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link Routes}.
 *
 * @since 0.2.0
 */
final class RoutesTest {

    @Test
    void fallsBackWhenNoRuleMatches() {
        Assertions.assertEquals(Routes.DEFAULT, new Routes().of("proj.tasks.add"));
    }

    @Test
    void matchesAWholeName() {
        Assertions.assertEquals(
            "important",
            new Routes().with("proj.tasks.add", "important").of("proj.tasks.add")
        );
    }

    @Test
    void refusesToMatchAnotherName() {
        Assertions.assertEquals(
            Routes.DEFAULT,
            new Routes().with("proj.tasks.add", "important").of("proj.tasks.mul")
        );
    }

    @Test
    void matchesANameThatStartsTheSameWay() {
        Assertions.assertEquals(
            "important",
            new Routes().with("proj.tasks.*", "important").of("proj.tasks.mul")
        );
    }

    @Test
    void refusesToMatchANameThatStartsDifferently() {
        Assertions.assertEquals(
            Routes.DEFAULT,
            new Routes().with("proj.tasks.*", "important").of("other.tasks.mul")
        );
    }

    @Test
    void triesTheRulesInTheOrderTheyWereAdded() {
        final Map<String, String> rules = new LinkedHashMap<>();
        rules.put("proj.tasks.add", "first");
        rules.put("proj.tasks.*", "second");
        Assertions.assertEquals(
            "first", new Routes(rules, Routes.DEFAULT).of("proj.tasks.add")
        );
    }

    @Test
    void keepsTheRulesItKnewWhenLearning() {
        Assertions.assertEquals(
            "important",
            new Routes().with("proj.tasks.*", "important")
                .with("other.*", "elsewhere")
                .of("proj.tasks.add")
        );
    }

    @Test
    void leavesTheRoutingItCameFromAlone() {
        final Routes routes = new Routes();
        routes.with("proj.tasks.*", "important");
        Assertions.assertEquals(Routes.DEFAULT, routes.of("proj.tasks.add"));
    }

    @Test
    void fallsBackWhereItWasTold() {
        Assertions.assertEquals(
            "elsewhere", new Routes().toward("elsewhere").of("proj.tasks.add")
        );
    }

    @Test
    void keepsItsRulesWhenTheFallbackChanges() {
        Assertions.assertEquals(
            "important",
            new Routes().with("proj.tasks.*", "important")
                .toward("elsewhere")
                .of("proj.tasks.add")
        );
    }
}
