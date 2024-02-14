/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2024
 * SPDX-License-Identifier: MIT
 */
/**
 * The port a message leaves and arrives through, and the adapters that speak
 * to a real broker.
 *
 * <p>A broker carries messages and knows nothing of tasks: what it sends is
 * what it receives, byte for byte.</p>
 *
 * @since 0.1.0
 */
package io.celery4j.transport;
