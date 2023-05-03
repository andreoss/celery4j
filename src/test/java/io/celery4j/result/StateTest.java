/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.result;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link State}.
 *
 * @since 0.1.0
 */
final class StateTest {

    @Test
    void namesItself() {
        Assertions.assertEquals(State.SUCCESS, new State(State.SUCCESS).name());
    }

    @Test
    void reportsASucceededTask() {
        Assertions.assertTrue(new State(State.SUCCESS).successful());
    }

    @Test
    void reportsATaskThatDidNotSucceed() {
        Assertions.assertFalse(new State(State.FAILURE).successful());
    }

    @Test
    void reportsAFailedTask() {
        Assertions.assertTrue(new State(State.FAILURE).failed());
    }

    @Test
    void reportsATaskThatDidNotFail() {
        Assertions.assertFalse(new State(State.STARTED).failed());
    }

    @Test
    void reportsAFinishedTask() {
        Assertions.assertTrue(new State(State.SUCCESS).terminal());
    }

    @Test
    void reportsACalledOffTaskAsFinished() {
        Assertions.assertTrue(new State(State.REVOKED).terminal());
    }

    @Test
    void reportsATaskStillRunning() {
        Assertions.assertFalse(new State(State.STARTED).terminal());
    }

    @Test
    void reportsATaskWaitingToRunAgain() {
        Assertions.assertFalse(new State(State.RETRY).terminal());
    }

    @Test
    void reportsATaskNothingIsKnownAbout() {
        Assertions.assertFalse(new State(State.PENDING).terminal());
    }

    @Test
    void reportsATaskAWorkerTook() {
        Assertions.assertFalse(new State(State.RECEIVED).terminal());
    }

    @Test
    void carriesAStateItDoesNotKnow() {
        Assertions.assertEquals("PROGRESS", new State("PROGRESS").name());
    }

    @Test
    void treatsAStateItDoesNotKnowAsUnfinished() {
        Assertions.assertFalse(new State("PROGRESS").terminal());
    }

    @Test
    void comparesEqualByName() {
        Assertions.assertEquals(new State(State.SUCCESS), new State(State.SUCCESS));
    }

    @Test
    void comparesEqualToItself() {
        final State state = new State(State.SUCCESS);
        Assertions.assertEquals(state, state);
    }

    @Test
    void comparesUnequalToAnotherState() {
        Assertions.assertNotEquals(new State(State.SUCCESS), new State(State.FAILURE));
    }

    @Test
    void comparesUnequalToAnotherKind() {
        Assertions.assertNotEquals(new State(State.SUCCESS), State.SUCCESS);
    }

    @Test
    void hashesByName() {
        Assertions.assertEquals(
            new State(State.SUCCESS).hashCode(), new State(State.SUCCESS).hashCode()
        );
    }

    @Test
    void describesItself() {
        Assertions.assertEquals(State.SUCCESS, new State(State.SUCCESS).toString());
    }
}
