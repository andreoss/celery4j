/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.worker;

import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageHeaders;
import io.celery4j.protocol.ProtocolException;
import io.celery4j.protocol.ProtocolV1;
import io.celery4j.protocol.ProtocolV2;
import io.celery4j.protocol.Task;
import io.celery4j.protocol.TaskBody;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for what follows a task when what the body names is not a call,
 * or when the version has no place to name one.
 *
 * @since 1.0.1
 */
final class FollowupsTest {

    /**
     * Queue the message came from.
     */
    private static final String QUEUE = "first";

    /**
     * Name of the task.
     */
    private static final String NAME = "proj.tasks.add";

    @Test
    void refusesAChainLinkThatIsNoCall() {
        Assertions.assertThrows(
            ProtocolException.class,
            () -> new Followups().after(FollowupsTest.chained(), 4)
        );
    }

    @Test
    void saysWhatWasThereInsteadOfACall() {
        Assertions.assertTrue(
            Assertions.assertThrows(
                ProtocolException.class,
                () -> new Followups().after(FollowupsTest.chained(), 4)
            ).getMessage().contains("signature")
        );
    }

    @Test
    void callsNothingForFailureUnderTheOlderVersion() {
        Assertions.assertEquals(List.of(), new Followups().failed(FollowupsTest.older()));
    }

    @Test
    void refusesToRetryAMessageOfTheOlderVersion() {
        Assertions.assertThrows(
            WorkerException.class, () -> Followups.retry(FollowupsTest.older(), new Options())
        );
    }

    private static Envelope chained() {
        final Task task = new Task("task-one", FollowupsTest.NAME, List.of(2, 2), Map.of());
        return new Envelope(
            new Message(
                new ProtocolV2().message(task, FollowupsTest.QUEUE).properties(),
                new ProtocolV2().message(task, FollowupsTest.QUEUE)
                    .headers()
                    .with(MessageHeaders.ROOT, null),
                new ProtocolV2().message(
                    task,
                    new TaskBody(
                        task.args(), task.kwargs(), Map.of(TaskBody.CHAIN, List.of("nonsense"))
                    ),
                    FollowupsTest.QUEUE
                ).body()
            ),
            FollowupsTest.QUEUE,
            task
        );
    }

    private static Envelope older() {
        final Task task = new Task("task-one", FollowupsTest.NAME, List.of(2, 2), Map.of());
        return new Envelope(
            new ProtocolV1().message(task, FollowupsTest.QUEUE), FollowupsTest.QUEUE, task
        );
    }
}
