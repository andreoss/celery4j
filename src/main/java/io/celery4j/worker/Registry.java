/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.worker;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * The names a worker answers for.
 *
 * <p>A name nobody registered is refused. A worker that ran an unregistered
 * task would be running whatever a producer named, which is not a worker but
 * a hole.</p>
 *
 * @since 0.1.0
 */
public final class Registry {

    /**
     * Jobs by task name.
     */
    private final Map<String, Job> jobs;

    /**
     * Ctor.
     *
     * @param jobs Jobs by task name
     */
    public Registry(final Map<String, Job> jobs) {
        this.jobs = jobs;
    }

    /**
     * The job a task name runs.
     *
     * @param name Name of the task
     * @return The job
     * @throws WorkerException If nobody registered that name
     */
    public Job of(final String name) throws WorkerException {
        final Job job = this.jobs.get(name);
        if (job == null) {
            throw new WorkerException(String.format("task %s is not registered", name));
        }
        return job;
    }

    /**
     * Whether a name is answered for.
     *
     * @param name Name of the task
     * @return True when it is
     */
    public boolean knows(final String name) {
        return this.jobs.containsKey(name);
    }

    /**
     * The same registry with one more name.
     *
     * @param name Name of the task
     * @param job What to run for it
     * @return A registry answering for that name
     */
    public Registry with(final String name, final Job job) {
        final Map<String, Job> extended = new LinkedHashMap<>(this.jobs);
        extended.put(name, job);
        return new Registry(extended);
    }

    /**
     * Every name answered for.
     *
     * @return The names
     */
    public Set<String> names() {
        return Set.copyOf(this.jobs.keySet());
    }
}
