/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.worker;

import io.celery4j.protocol.Message;
import io.celery4j.protocol.Task;

/**
 * What a worker is holding while it runs one task: the message it came in, the
 * queue it came from, and the task it asks for.
 *
 * @param message Message as the broker delivered it
 * @param queue Queue it came from, and the one a retry goes back to
 * @param task Task the message asks for
 * @since 0.1.1
 */
record Envelope(Message message, String queue, Task task) {
}
