/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2024
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.client;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Which queue a task name goes to.
 *
 * <p>A rule is either a whole name or a name ending in a star, which matches
 * every name that starts the same way. Rules are tried in the order they were
 * added, and a name that matches none goes to the queue this falls back to.
 * That is all the routing a caller needs to stop repeating queue names.</p>
 *
 * <p>What it is given is held as given, and what it hands out is a view of
 * that which refuses to be written to. A caller that keeps hold of the
 * collection it passed, and changes it later, changes what this holds; pass
 * one nobody else keeps.</p>
 *
 * @since 0.2.0
 */
public final class Routes {

    /**
     * Queue a name goes to when no rule matches and none was given.
     */
    public static final String DEFAULT = "celery";

    /**
     * Queue by rule, in the order the rules are tried.
     */
    private final Map<String, String> rules;

    /**
     * Queue a name goes to when no rule matches.
     */
    private final String fallback;

    /**
     * Ctor.
     */
    public Routes() {
        this(Map.of(), Routes.DEFAULT);
    }

    /**
     * Ctor.
     *
     * @param rules Queue by rule, in the order the rules are tried
     * @param fallback Queue a name goes to when no rule matches
     */
    public Routes(final Map<String, String> rules, final String fallback) {
        this.rules = rules;
        this.fallback = fallback;
    }

    /**
     * Queue a task name goes to.
     *
     * @param name Name of the task
     * @return The queue
     */
    public String of(final String name) {
        String queue = this.fallback;
        for (final Map.Entry<String, String> rule : this.rules.entrySet()) {
            if (Routes.matches(rule.getKey(), name)) {
                queue = rule.getValue();
                break;
            }
        }
        return queue;
    }

    /**
     * The same routing with one more rule, tried after the ones already there.
     *
     * @param rule Whole name, or a name ending in a star
     * @param queue Queue a matching name goes to
     * @return Routing that knows that rule
     */
    public Routes with(final String rule, final String queue) {
        final Map<String, String> extended = new LinkedHashMap<>(this.rules);
        extended.put(rule, queue);
        return new Routes(extended, this.fallback);
    }

    /**
     * The same routing, falling back to another queue.
     *
     * @param queue Queue a name goes to when no rule matches
     * @return Routing that falls back there
     */
    public Routes toward(final String queue) {
        return new Routes(this.rules, queue);
    }

    private static boolean matches(final String rule, final String name) {
        final boolean same;
        if (rule.endsWith("*")) {
            same = name.startsWith(rule.substring(0, rule.length() - 1));
        } else {
            same = rule.equals(name);
        }
        return same;
    }
}
