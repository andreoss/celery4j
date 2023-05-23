/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.stack;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import io.celery4j.protocol.Message;
import io.celery4j.protocol.ProtocolV2;
import io.celery4j.protocol.Task;
import io.celery4j.result.Backend;
import io.celery4j.result.RedisBackend;
import io.celery4j.result.State;
import io.celery4j.result.TaskResult;
import io.celery4j.transport.AmqpBroker;
import io.celery4j.transport.Broker;
import io.celery4j.transport.RedisBroker;
import io.celery4j.transport.TransportException;
import io.celery4j.worker.Registry;
import io.celery4j.worker.Worker;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.JedisPool;

/**
 * Live test case for services reached the way a deployment reaches them.
 *
 * <p>Every other live case talks to a store with no password on its first
 * database and to a queue service on the default virtual host with the
 * credentials it was born with. No deployment looks like that, and the parts
 * of a connection nobody exercises are the parts that turn out to be wrong, so
 * these cases secure both services and check that what is written lands where
 * the connection said it would and nowhere else.</p>
 *
 * @since 1.0.0
 */
@Tag("live")
@Testcontainers
final class ConfiguredServicesIT {

    /**
     * Port the store listens on inside its container.
     */
    private static final int PORT = 6379;

    /**
     * Password the store wants.
     */
    private static final String SECRET = "st0re-secret";

    /**
     * Database the cases use, which is not the first one.
     */
    private static final int DATABASE = 3;

    /**
     * Virtual host the queue cases use.
     */
    private static final String VHOST = "orders";

    /**
     * User the queue service knows, which is not the one it was born with.
     */
    private static final String USER = "carrier";

    /**
     * Password of that user.
     */
    private static final String PASSWORD = "qu3ue-secret";

    /**
     * How long a receive waits.
     */
    private static final Duration WAIT = Duration.ofSeconds(3L);

    /**
     * The store the cases run against, which wants a password.
     */
    @Container
    private static final GenericContainer<?> STORE =
        new GenericContainer<>("docker.io/library/redis:7-alpine")
            .withExposedPorts(ConfiguredServicesIT.PORT)
            .withCommand("redis-server", "--requirepass", ConfiguredServicesIT.SECRET);

    /**
     * The queue service the cases run against, which has a virtual host and a
     * user of its own.
     */
    @Container
    private static final RabbitMQContainer MESSAGES = ConfiguredServicesIT.service();

    @BeforeAll
    static void configure() {
        ConfiguredServicesIT.ctl("add_vhost", ConfiguredServicesIT.VHOST);
        ConfiguredServicesIT.ctl(
            "add_user", ConfiguredServicesIT.USER, ConfiguredServicesIT.PASSWORD
        );
        ConfiguredServicesIT.ctl(
            "set_permissions",
            "-p",
            ConfiguredServicesIT.VHOST,
            ConfiguredServicesIT.USER,
            ".*",
            ".*",
            ".*"
        );
    }

    @Test
    void carriesATaskOverAStoreThatWantsAPassword() {
        final String queue = ConfiguredServicesIT.queue();
        try (Broker broker = new RedisBroker(ConfiguredServicesIT.pool())) {
            broker.send(ConfiguredServicesIT.message(queue), queue);
            Assertions.assertEquals(
                List.of(2, 3),
                new ProtocolV2()
                    .task(broker.receive(queue, ConfiguredServicesIT.WAIT).orElseThrow())
                    .args()
            );
        }
    }

    @Test
    void keepsAResultOnADatabaseOtherThanTheFirst() {
        final String id = UUID.randomUUID().toString();
        try (Backend backend = new RedisBackend(ConfiguredServicesIT.pool())) {
            backend.store(ConfiguredServicesIT.result(id));
            Assertions.assertEquals(
                Optional.of(42), backend.of(id).orElseThrow().value()
            );
        }
    }

    @Test
    void leavesTheFirstDatabaseEmpty() {
        final String queue = ConfiguredServicesIT.queue();
        try (Broker broker = new RedisBroker(ConfiguredServicesIT.pool())) {
            broker.send(ConfiguredServicesIT.message(queue), queue);
        }
        try (Broker first = new RedisBroker(ConfiguredServicesIT.pool(0))) {
            Assertions.assertEquals(
                Optional.empty(), first.receive(queue, ConfiguredServicesIT.WAIT)
            );
        }
    }

    @Test
    void refusesAStoreThatWasGivenNoPassword() {
        final String queue = ConfiguredServicesIT.queue();
        try (
            Broker broker = new RedisBroker(
                new JedisPool(
                    URI.create(
                        String.format(
                            "redis://%s:%d/%d",
                            ConfiguredServicesIT.STORE.getHost(),
                            ConfiguredServicesIT.STORE.getMappedPort(
                                ConfiguredServicesIT.PORT
                            ),
                            ConfiguredServicesIT.DATABASE
                        )
                    )
                )
            )
        ) {
            Assertions.assertThrows(
                TransportException.class,
                () -> broker.send(ConfiguredServicesIT.message(queue), queue)
            );
        }
    }

    @Test
    void leavesASucceededResultOverASecuredStore() {
        Assertions.assertEquals(
            new State(State.SUCCESS), ConfiguredServicesIT.ran(UUID.randomUUID().toString())
        );
    }

    @Test
    void carriesATaskOverAVirtualHostOfItsOwn() {
        final String queue = ConfiguredServicesIT.queue();
        try (
            Broker broker = new AmqpBroker(
                ConfiguredServicesIT.channel(ConfiguredServicesIT.VHOST)
            )
        ) {
            broker.send(ConfiguredServicesIT.message(queue), queue);
            Assertions.assertEquals(
                List.of(2, 3),
                new ProtocolV2()
                    .task(broker.receive(queue, ConfiguredServicesIT.WAIT).orElseThrow())
                    .args()
            );
        }
    }

    @Test
    void leavesTheDefaultVirtualHostEmpty() {
        final String queue = ConfiguredServicesIT.queue();
        try (
            Broker broker = new AmqpBroker(
                ConfiguredServicesIT.channel(ConfiguredServicesIT.VHOST)
            )
        ) {
            broker.send(ConfiguredServicesIT.message(queue), queue);
        }
        try (Broker other = new AmqpBroker(ConfiguredServicesIT.admin())) {
            Assertions.assertEquals(
                Optional.empty(), other.receive(queue, ConfiguredServicesIT.WAIT)
            );
        }
    }

    @Test
    void refusesCredentialsTheQueueServiceDoesNotKnow() {
        Assertions.assertThrows(
            TransportException.class,
            () -> ConfiguredServicesIT.channel(
                ConfiguredServicesIT.VHOST, ConfiguredServicesIT.USER, "not-the-password"
            )
        );
    }

    private static State ran(final String id) {
        final String queue = ConfiguredServicesIT.queue();
        try (
            Broker broker = new RedisBroker(ConfiguredServicesIT.pool());
            Backend backend = new RedisBackend(ConfiguredServicesIT.pool())
        ) {
            broker.send(
                new ProtocolV2().message(
                    new Task(id, "proj.java.sum", List.of(2, 3), Map.of()), queue
                ),
                queue
            );
            if (
                new Worker(
                    broker, backend, new Registry(Map.of("proj.java.sum", task -> 5))
                ).once(queue, ConfiguredServicesIT.WAIT).isEmpty()
            ) {
                throw new IllegalStateException("the worker took nothing");
            }
            return backend.of(id).orElseThrow().state();
        }
    }

    private static Channel admin() {
        return ConfiguredServicesIT.channel(
            "/",
            ConfiguredServicesIT.MESSAGES.getAdminUsername(),
            ConfiguredServicesIT.MESSAGES.getAdminPassword()
        );
    }

    private static void ctl(final String... arguments) {
        final String[] command = new String[arguments.length + 1];
        command[0] = "rabbitmqctl";
        System.arraycopy(arguments, 0, command, 1, arguments.length);
        try {
            final ExecResult done = ConfiguredServicesIT.MESSAGES.execInContainer(command);
            if (done.getExitCode() != 0) {
                throw new IllegalStateException(
                    String.format("the queue service refused %s", done.getStderr())
                );
            }
        } catch (final IOException ex) {
            throw new IllegalStateException("the queue service cannot be configured", ex);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("configuring the queue service was interrupted", ex);
        }
    }

    private static RabbitMQContainer service() {
        return new RabbitMQContainer(
            DockerImageName.parse("docker.io/library/rabbitmq:3.13-alpine")
                .asCompatibleSubstituteFor("rabbitmq")
        );
    }

    private static Message message(final String queue) {
        return new ProtocolV2().message(
            new Task(UUID.randomUUID().toString(), "proj.java.sum", List.of(2, 3), Map.of()),
            queue
        );
    }

    private static TaskResult result(final String id) {
        return new TaskResult(
            Map.of(
                TaskResult.ID, id,
                TaskResult.STATUS, State.SUCCESS,
                TaskResult.VALUE, 42,
                TaskResult.CHILDREN, List.of()
            )
        );
    }

    private static String queue() {
        return String.format("configured-%s", UUID.randomUUID());
    }

    private static JedisPool pool() {
        return ConfiguredServicesIT.pool(ConfiguredServicesIT.DATABASE);
    }

    private static JedisPool pool(final int database) {
        return new JedisPool(
            URI.create(
                String.format(
                    "redis://:%s@%s:%d/%d",
                    ConfiguredServicesIT.SECRET,
                    ConfiguredServicesIT.STORE.getHost(),
                    ConfiguredServicesIT.STORE.getMappedPort(ConfiguredServicesIT.PORT),
                    database
                )
            )
        );
    }

    private static Channel channel(final String vhost) {
        return ConfiguredServicesIT.channel(
            vhost, ConfiguredServicesIT.USER, ConfiguredServicesIT.PASSWORD
        );
    }

    private static Channel channel(
        final String vhost, final String user, final String password
    ) {
        final ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(ConfiguredServicesIT.MESSAGES.getHost());
        factory.setPort(ConfiguredServicesIT.MESSAGES.getAmqpPort());
        factory.setVirtualHost(vhost);
        factory.setUsername(user);
        factory.setPassword(password);
        try {
            return factory.newConnection().createChannel();
        } catch (final IOException ex) {
            throw new TransportException("the queue service cannot be reached", ex);
        } catch (final TimeoutException ex) {
            throw new TransportException("the queue service took too long to answer", ex);
        }
    }
}
