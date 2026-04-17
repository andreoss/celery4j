/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.transport;

import java.util.Comparator;
import java.util.List;

/**
 * The names a queue is known by, one per priority step.
 *
 * <p>A broker without priorities of its own gets them from the queue name: the
 * base name carries the lowest step, and every other step appends its number
 * after a separator. A priority between two steps belongs to the lower one, so
 * that a producer and a consumer that disagree on the exact number still meet
 * on the same queue.</p>
 *
 * @since 0.1.0
 */
public final class Queues {

    /**
     * Separator between a queue name and its priority step, which is the pair
     * of control characters this transport prescribes.
     */
    public static final String SEPARATOR = new String(new char[] {6, 22});

    /**
     * Priority steps a queue is split into, lowest first.
     */
    private static final List<Integer> STEPS = List.of(0, 3, 6, 9);

    /**
     * Separator between a queue name and its priority step.
     */
    private final String separator;

    /**
     * Priority steps a queue is split into, lowest first.
     */
    private final List<Integer> steps;

    /**
     * Ctor.
     */
    public Queues() {
        this(Queues.SEPARATOR, Queues.STEPS);
    }

    /**
     * Ctor.
     *
     * @param separator Separator between a queue name and its priority step
     * @param steps Priority steps a queue is split into, lowest first
     */
    public Queues(final String separator, final List<Integer> steps) {
        this.separator = separator;
        this.steps = steps;
    }

    /**
     * Name of the queue a priority is carried on.
     *
     * @param queue Base name of the queue
     * @param priority Priority asked for
     * @return The name to send to and to read from
     */
    public String named(final String queue, final long priority) {
        final long step = this.step(priority);
        final String name;
        if (step == 0L) {
            name = queue;
        } else {
            name = String.format("%s%s%d", queue, this.separator, step);
        }
        return name;
    }

    /**
     * Every name a queue is known by, highest priority first, which is the
     * order a worker reads them in.
     *
     * @param queue Base name of the queue
     * @return The names
     */
    public List<String> all(final String queue) {
        return this.steps.stream()
            .sorted(Comparator.reverseOrder())
            .map(step -> this.named(queue, step))
            .toList();
    }

    private long step(final long priority) {
        long found = this.steps.get(0);
        for (final Integer step : this.steps) {
            if (step <= priority) {
                found = step;
            }
        }
        return found;
    }
}
