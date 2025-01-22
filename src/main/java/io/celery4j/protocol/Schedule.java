/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * When a task may start and when it stops being worth running.
 *
 * <p>Both times are written as ISO 8601 text. A time without a zone is read as
 * coordinated universal time, which is what this protocol version prescribes
 * since it dropped its flag for saying so.</p>
 *
 * @since 0.1.0
 */
public final class Schedule {

    /**
     * Headers the times are read from.
     */
    private final MessageHeaders headers;

    /**
     * Ctor.
     *
     * @param headers Headers the times are read from
     */
    public Schedule(final MessageHeaders headers) {
        this.headers = headers;
    }

    /**
     * Time the task may start at.
     *
     * @return The time, empty when the task may start at once
     * @throws ProtocolException If the header is no time
     */
    public Optional<Instant> eta() throws ProtocolException {
        return this.time(MessageHeaders.ETA);
    }

    /**
     * Time after which the task is no longer worth running.
     *
     * @return The time, empty when the task never expires
     * @throws ProtocolException If the header is no time
     */
    public Optional<Instant> expires() throws ProtocolException {
        return this.time(MessageHeaders.EXPIRES);
    }

    /**
     * Whether the task stopped being worth running.
     *
     * @param now Time to judge against
     * @return True when the task expired before that time
     * @throws ProtocolException If the header is no time
     */
    public boolean expired(final Instant now) throws ProtocolException {
        return this.expires().filter(time -> !time.isAfter(now)).isPresent();
    }

    /**
     * Whether the task may start.
     *
     * @param now Time to judge against
     * @return True when no start time was asked for, or it has passed
     * @throws ProtocolException If the header is no time
     */
    public boolean due(final Instant now) throws ProtocolException {
        return this.eta().filter(time -> time.isAfter(now)).isEmpty();
    }

    /**
     * The same headers, asking the task to start no earlier than a time.
     *
     * @param eta Time the task may start at
     * @return Headers carrying that time
     */
    public MessageHeaders at(final Instant eta) {
        return this.headers.with(MessageHeaders.ETA, Schedule.text(eta));
    }

    /**
     * The same headers, asking the task to be discarded after a time.
     *
     * @param expires Time after which the task is no longer worth running
     * @return Headers carrying that time
     */
    public MessageHeaders until(final Instant expires) {
        return this.headers.with(MessageHeaders.EXPIRES, Schedule.text(expires));
    }

    /**
     * How long the task may run for.
     *
     * <p>The protocol carries a soft and a hard limit. The soft one is what a
     * worker acts on first, and the hard one is what it falls back to when no
     * soft one was named.</p>
     *
     * @return The limit, empty when the message names none
     * @throws ProtocolException If the header is no pair of numbers
     */
    public Optional<Duration> limit() throws ProtocolException {
        final Object value = this.headers.asMap().get(MessageHeaders.TIMELIMIT);
        Optional<Duration> found = Optional.empty();
        if (value instanceof List<?> pair) {
            found = pair.stream()
                .filter(Objects::nonNull)
                .map(Schedule::seconds)
                .findFirst();
        } else if (value != null) {
            throw new ProtocolException(
                String.format("header %s is no pair: %s", MessageHeaders.TIMELIMIT, value)
            );
        }
        return found;
    }

    private Optional<Instant> time(final String name) {
        return this.headers.text(name).map(Schedule::instant);
    }

    private static Duration seconds(final Object value) {
        if (!(value instanceof Number number)) {
            throw new ProtocolException(
                String.format("a time limit is no number: %s", value)
            );
        }
        return Duration.ofMillis(Math.round(number.doubleValue() * 1000.0));
    }

    private static String text(final Instant time) {
        return DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ssXXX")
            .withZone(ZoneOffset.UTC)
            .format(time);
    }

    private static Instant instant(final String text) {
        Instant time;
        try {
            time = OffsetDateTime.parse(text).toInstant();
        } catch (final DateTimeParseException ex) {
            time = Schedule.local(text);
        }
        return time;
    }

    private static Instant local(final String text) {
        try {
            return LocalDateTime.parse(text).toInstant(ZoneOffset.UTC);
        } catch (final DateTimeParseException ex) {
            throw new ProtocolException(String.format("time is no ISO 8601 text: %s", text), ex);
        }
    }
}
