/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.transport;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link Exchange}.
 *
 * @since 1.0.0
 */
final class ExchangeTest {

    @Test
    void hasNoNameWhenNobodyGaveItOne() {
        Assertions.assertEquals(Exchange.NONE, new Exchange().name());
    }

    @Test
    void declaresTheQueuesItRoutesToByName() {
        Assertions.assertTrue(new Exchange().declares());
    }

    @Test
    void keepsTheNameItWasGiven() {
        Assertions.assertEquals("orders", new Exchange("orders").name());
    }

    @Test
    void leavesTheQueuesOfANamedExchangeAlone() {
        Assertions.assertFalse(new Exchange("orders").declares());
    }

    @Test
    void declaresWhereItWasToldTo() {
        Assertions.assertTrue(new Exchange("orders", true).declares());
    }

    @Test
    void leavesQueuesAloneWhereItWasToldTo() {
        Assertions.assertFalse(new Exchange(Exchange.NONE, false).declares());
    }
}
