/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.worker;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Several workers taking from the same queues until they are told to stop.
 *
 * <p>How many run together is how many the executor can run at once: nothing
 * here starts a thread of its own, so a caller decides what the bound is and
 * owns the threads. Each of them takes one message at a time, so the number of
 * tasks running together never exceeds the number of runs asked for.</p>
 *
 * <p>Stopping is asked for, not forced: a worker holding a task finishes it,
 * and then sees that it should stop. A run therefore ends no later than one
 * receive timeout plus one task after it was asked to. Closing a fleet asks it
 * to stop and then releases the threads it was given.</p>
 *
 * @since 0.2.0
 */
public final class Fleet implements AutoCloseable {

    /**
     * Worker each run uses.
     */
    private final Worker worker;

    /**
     * Threads the runs are given to.
     */
    private final ExecutorService threads;

    /**
     * Whether a stop has been asked for.
     */
    private final AtomicBoolean stopped;

    /**
     * Ctor.
     *
     * @param worker Worker each run uses
     * @param threads Threads the runs are given to
     * @param stopped Whether a stop has been asked for
     */
    public Fleet(
        final Worker worker, final ExecutorService threads, final AtomicBoolean stopped
    ) {
        this.worker = worker;
        this.threads = threads;
        this.stopped = stopped;
    }

    /**
     * Take messages until a stop is asked for.
     *
     * @param queues Queues to read from, in the order they are served
     * @param timeout How long each receive waits
     * @param runs How many workers take from the queues at once
     * @return How many messages were seen through, across all runs
     * @throws WorkerException If a run ended in a failure of its own
     */
    public long run(final List<String> queues, final Duration timeout, final int runs)
        throws WorkerException {
        final List<Future<Long>> started = new ArrayList<>(runs);
        for (int index = 0; index < runs; index = index + 1) {
            started.add(this.threads.submit(() -> this.loop(queues, timeout)));
        }
        long handled = 0L;
        for (final Future<Long> run : started) {
            handled = handled + Fleet.awaited(run);
        }
        return handled;
    }

    /**
     * Ask the runs to stop. They end after what they are holding.
     */
    public void stop() {
        this.stopped.set(true);
    }

    /**
     * Whether a stop has been asked for.
     *
     * @return True once it has
     */
    public boolean stopping() {
        return this.stopped.get();
    }

    @Override
    public void close() {
        this.stop();
        this.threads.shutdown();
    }

    private long loop(final List<String> queues, final Duration timeout) {
        long handled = 0L;
        while (!this.stopped.get() && !Thread.currentThread().isInterrupted()) {
            if (this.worker.once(queues, timeout).isPresent()) {
                handled = handled + 1L;
            }
        }
        return handled;
    }

    private static long awaited(final Future<Long> run) {
        try {
            return run.get();
        } catch (final ExecutionException ex) {
            throw new WorkerException("a run ended in a failure", ex);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new WorkerException("waiting for a run was interrupted", ex);
        }
    }
}
