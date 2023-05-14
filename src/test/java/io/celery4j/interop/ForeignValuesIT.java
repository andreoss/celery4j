/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.interop;

import io.celery4j.protocol.ProtocolV2;
import io.celery4j.protocol.Task;
import io.celery4j.result.Backend;
import io.celery4j.result.RedisBackend;
import io.celery4j.result.Results;
import io.celery4j.transport.Broker;
import io.celery4j.transport.RedisBroker;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import redis.clients.jedis.JedisPool;

/**
 * Live test case for what a value is worth on the way to a foreign worker and
 * back.
 *
 * <p>A task takes and returns more than whole numbers. A fraction that arrives
 * rounded, text that loses its accents, or a map that arrives flattened would
 * pass every case that only ever sends an integer, which is why each kind has
 * a case of its own here.</p>
 *
 * @since 0.2.1
 */
@Tag("live")
@Testcontainers
final class ForeignValuesIT {

    /**
     * Port the store listens on inside its container.
     */
    private static final int PORT = 6379;

    /**
     * Queue the foreign worker reads.
     */
    private static final String QUEUE = "interop";

    /**
     * Name of the task that hands back what it was given.
     */
    private static final String ECHO = "proj.tasks.echo";

    /**
     * Name of the task that hands back a value of a named kind.
     */
    private static final String GIVE = "proj.tasks.give";

    /**
     * How long a foreign worker may take to start and finish a task.
     */
    private static final Duration PATIENCE = Duration.ofMinutes(2L);

    /**
     * Network the store and the worker meet on.
     */
    private static final Network NETWORK = Network.newNetwork();

    /**
     * The store both sides use.
     */
    @Container
    private static final GenericContainer<?> STORE =
        new GenericContainer<>("docker.io/library/redis:7-alpine")
            .withExposedPorts(ForeignValuesIT.PORT)
            .withNetwork(ForeignValuesIT.NETWORK)
            .withNetworkAliases("store");

    /**
     * A worker of the original implementation.
     */
    @Container
    private static final GenericContainer<?> FOREIGN = ForeignValuesIT.foreign();

    @Test
    void carriesAWholeNumberThere() {
        Assertions.assertEquals(List.of(7), ForeignValuesIT.echoed(List.of(7)));
    }

    @Test
    void carriesAFractionThere() {
        Assertions.assertEquals(List.of(2.5), ForeignValuesIT.echoed(List.of(2.5)));
    }

    @Test
    void carriesALargeNumberThere() {
        Assertions.assertEquals(
            List.of(9_007_199_254_740_993L), ForeignValuesIT.echoed(List.of(9_007_199_254_740_993L))
        );
    }

    @Test
    void carriesTextWithAccentsThere() {
        Assertions.assertEquals(
            List.of("héllo wörld"), ForeignValuesIT.echoed(List.of("héllo wörld"))
        );
    }

    @Test
    void carriesTrueThere() {
        Assertions.assertEquals(List.of(true), ForeignValuesIT.echoed(List.of(true)));
    }

    @Test
    void carriesFalseThere() {
        Assertions.assertEquals(List.of(false), ForeignValuesIT.echoed(List.of(false)));
    }

    @Test
    void carriesNothingThere() {
        Assertions.assertEquals(
            Collections.singletonList(null),
            ForeignValuesIT.echoed(Collections.singletonList(null))
        );
    }

    @Test
    void carriesAListThere() {
        Assertions.assertEquals(
            List.of(List.of(1, 2, 3)), ForeignValuesIT.echoed(List.of(List.of(1, 2, 3)))
        );
    }

    @Test
    void carriesAMapThere() {
        Assertions.assertEquals(
            List.of(Map.of("a", 1, "b", List.of(2, 3))),
            ForeignValuesIT.echoed(List.of(Map.of("a", 1, "b", List.of(2, 3))))
        );
    }

    @Test
    void carriesSeveralParametersThere() {
        Assertions.assertEquals(
            Arrays.asList(1, "two", 3.5), ForeignValuesIT.echoed(Arrays.asList(1, "two", 3.5))
        );
    }

    @Test
    void carriesNamedParametersThere() {
        Assertions.assertEquals(
            Map.of("args", List.of(), "kwargs", Map.of("left", 2, "right", "three")),
            ForeignValuesIT.answered(
                ForeignValuesIT.ECHO, List.of(), Map.of("left", 2, "right", "three")
            )
        );
    }

    @Test
    void readsAWholeNumberBack() {
        Assertions.assertEquals(7, ForeignValuesIT.given("whole"));
    }

    @Test
    void readsAFractionBack() {
        Assertions.assertEquals(2.5, ForeignValuesIT.given("fraction"));
    }

    @Test
    void readsTextWithAccentsBack() {
        Assertions.assertEquals("héllo wörld", ForeignValuesIT.given("text"));
    }

    @Test
    void readsTrueBack() {
        Assertions.assertEquals(true, ForeignValuesIT.given("yes"));
    }

    @Test
    void readsFalseBack() {
        Assertions.assertEquals(false, ForeignValuesIT.given("no"));
    }

    @Test
    void readsNothingBack() {
        Assertions.assertEquals(
            Optional.empty(),
            ForeignValuesIT.result(ForeignValuesIT.GIVE, List.of("nothing"), Map.of())
        );
    }

    @Test
    void readsAListBack() {
        Assertions.assertEquals(List.of(1, "two", 3.5), ForeignValuesIT.given("list"));
    }

    @Test
    void readsAMapBack() {
        Assertions.assertEquals(
            Map.of("a", 1, "b", List.of(2, 3), "c", Map.of("d", "e")),
            ForeignValuesIT.given("map")
        );
    }

    @Test
    void keepsAForeignWorkerRunning() {
        Assertions.assertTrue(ForeignValuesIT.FOREIGN.isRunning());
    }

    private static Object echoed(final List<Object> args) {
        return ForeignValuesIT.answered(ForeignValuesIT.ECHO, args, Map.of());
    }

    private static Object given(final String kind) {
        return ForeignValuesIT.answered(ForeignValuesIT.GIVE, List.of(kind), Map.of());
    }

    private static Object answered(
        final String name, final List<Object> args, final Map<String, Object> kwargs
    ) {
        return ForeignValuesIT.result(name, args, kwargs).orElseThrow();
    }

    private static Optional<Object> result(
        final String name, final List<Object> args, final Map<String, Object> kwargs
    ) {
        final String id = UUID.randomUUID().toString();
        try (
            Broker broker = new RedisBroker(ForeignValuesIT.pool());
            Backend backend = new RedisBackend(ForeignValuesIT.pool())
        ) {
            broker.send(
                new ProtocolV2().message(
                    new Task(id, name, args, kwargs), ForeignValuesIT.QUEUE
                ),
                ForeignValuesIT.QUEUE
            );
            return new Results(backend, Duration.ofMillis(200L))
                .await(id, ForeignValuesIT.PATIENCE)
                .orElseThrow()
                .value();
        }
    }

    private static GenericContainer<?> foreign() {
        return new GenericContainer<>(ForeignValuesIT.image())
            .withNetwork(ForeignValuesIT.NETWORK)
            .withEnv("BROKER_URL", "redis://store:6379/0")
            .withStartupTimeout(Duration.ofMinutes(5L));
    }

    private static ImageFromDockerfile image() {
        return new ImageFromDockerfile()
            .withFileFromClasspath("Dockerfile", "live/Dockerfile")
            .withFileFromClasspath("tasks.py", "live/tasks.py")
            .withFileFromClasspath("producer.py", "live/producer.py")
            .withFileFromClasspath("schedule.py", "live/schedule.py")
            .withFileFromClasspath("serialize.py", "live/serialize.py");
    }

    private static JedisPool pool() {
        return new JedisPool(
            ForeignValuesIT.STORE.getHost(),
            ForeignValuesIT.STORE.getMappedPort(ForeignValuesIT.PORT)
        );
    }
}
