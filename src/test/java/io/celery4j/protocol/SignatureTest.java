/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2024
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link Signature}.
 *
 * @since 1.0.0
 */
final class SignatureTest {

    /**
     * Name of the task the calls here ask for.
     */
    private static final String NAME = "proj.tasks.add";

    @Test
    void readsTheTaskItAsksFor() {
        Assertions.assertEquals(SignatureTest.NAME, SignatureTest.call().name());
    }

    @Test
    void refusesFieldsThatNameNoTask() {
        Assertions.assertThrows(
            ProtocolException.class, () -> new Signature(Map.of()).name()
        );
    }

    @Test
    void keepsTheIdentityItWasGiven() {
        Assertions.assertEquals("task-two", SignatureTest.call().id());
    }

    @Test
    void inventsAnIdentityWhenItIsGivenNone() {
        Assertions.assertNotEquals(
            new Signature(Map.of(Signature.TASK, SignatureTest.NAME)).id(),
            new Signature(Map.of(Signature.TASK, SignatureTest.NAME)).id()
        );
    }

    @Test
    void readsTheArgumentsItAlreadyCarries() {
        Assertions.assertEquals(List.of(10), SignatureTest.call().args());
    }

    @Test
    void readsNoArgumentsWhenItCarriesNone() {
        Assertions.assertEquals(
            List.of(), new Signature(Map.of(Signature.TASK, SignatureTest.NAME)).args()
        );
    }

    @Test
    void readsTheKeywordArgumentsItAlreadyCarries() {
        Assertions.assertEquals(Map.of("debug", true), SignatureTest.call().kwargs());
    }

    @Test
    void readsTheQueueItGoesTo() {
        Assertions.assertEquals(Optional.of("second"), SignatureTest.call().queue());
    }

    @Test
    void namesNoQueueWhenItWasGivenNone() {
        Assertions.assertEquals(
            Optional.empty(), new Signature(Map.of(Signature.TASK, SignatureTest.NAME)).queue()
        );
    }

    @Test
    void putsWhatCameBeforeInFrontOfItsArguments() {
        Assertions.assertEquals(List.of(4, 10), SignatureTest.call().task(4).args());
    }

    @Test
    void leavesTheArgumentsOfAnImmutableCallAlone() {
        Assertions.assertEquals(
            List.of(10),
            new Signature(
                Map.of(
                    Signature.TASK, SignatureTest.NAME,
                    Signature.ARGS, List.of(10),
                    Signature.IMMUTABLE, true
                )
            ).task(4).args()
        );
    }

    @Test
    void asksForTheTaskItNames() {
        Assertions.assertEquals(SignatureTest.NAME, SignatureTest.call().task(4).name());
    }

    @Test
    void readsTheOptionsItWasGiven() {
        Assertions.assertEquals(
            "second", SignatureTest.call().options().get(Signature.QUEUE)
        );
    }

    @Test
    void handsOutItsFields() {
        Assertions.assertEquals(
            SignatureTest.NAME, SignatureTest.call().asMap().get(Signature.TASK)
        );
    }

    private static Signature call() {
        return new Signature(
            Map.of(
                Signature.TASK, SignatureTest.NAME,
                Signature.ARGS, List.of(10),
                Signature.KWARGS, Map.of("debug", true),
                Signature.OPTIONS,
                Map.of(Signature.ID, "task-two", Signature.QUEUE, "second")
            )
        );
    }
}
