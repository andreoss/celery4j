/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.client;

import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for routing that stays what it was.
 *
 * @since 1.0.1
 */
final class UnchangeableRoutesTest {

    @Test
    void keepsItsRulesWhenOneMoreIsAskedFor() {
        final Routes routes = new Routes(Map.of("proj.*", "important"), Routes.DEFAULT);
        routes.with("other.*", "elsewhere");
        Assertions.assertEquals(Routes.DEFAULT, routes.of("other.tasks.add"));
    }

    @Test
    void keepsItsFallbackWhenAnotherIsAskedFor() {
        final Routes routes = new Routes(Map.of("proj.*", "important"), Routes.DEFAULT);
        routes.toward("elsewhere");
        Assertions.assertEquals(Routes.DEFAULT, routes.of("other.tasks.add"));
    }
}
