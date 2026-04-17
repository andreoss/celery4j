/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.result;

import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import redis.clients.jedis.JedisPool;

/**
 * Live test case for {@link RedisBackend}, against a store in a container.
 *
 * @since 0.1.0
 */
@Tag("live")
@Testcontainers
final class RedisBackendIT implements BackendContract {

    /**
     * Port the store listens on inside its container.
     */
    private static final int PORT = 6379;

    /**
     * The store the cases run against.
     */
    @Container
    private static final GenericContainer<?> STORE =
        new GenericContainer<>("docker.io/library/redis:7-alpine")
            .withExposedPorts(RedisBackendIT.PORT);

    @Override
    public Backend backend() {
        return new RedisBackend(RedisBackendIT.pool());
    }

    @Override
    public String id() {
        return "live-task";
    }

    @Test
    void holdsAResultUnderTheKeyTheProtocolNames() {
        try (Backend backend = this.backend()) {
            backend.store(this.result("keyed", State.SUCCESS));
            Assertions.assertTrue(
                RedisBackendIT.pool().getResource()
                    .exists(String.format("%s%s", RedisBackend.PREFIX, this.id("keyed")))
            );
        }
    }

    @Test
    void forgetsAResultWhenItsLifetimeRanOut() {
        try (
            Backend backend = new RedisBackend(
                RedisBackendIT.pool(), new JsonResults(), Duration.ofSeconds(1L)
            )
        ) {
            backend.store(this.result("brief", State.SUCCESS));
            Assertions.assertEquals(
                Optional.empty(), RedisBackendIT.later(backend, this.id("brief"))
            );
        }
    }

    private static JedisPool pool() {
        return new JedisPool(
            RedisBackendIT.STORE.getHost(),
            RedisBackendIT.STORE.getMappedPort(RedisBackendIT.PORT)
        );
    }

    private static Optional<TaskResult> later(final Backend backend, final String id) {
        try {
            Thread.sleep(1500L);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ResultException("waiting was interrupted", ex);
        }
        return backend.of(id);
    }
}
