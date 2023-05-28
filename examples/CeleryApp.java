import io.celery4j.client.Client;
import io.celery4j.result.Backend;
import io.celery4j.result.RedisBackend;
import io.celery4j.transport.Broker;
import io.celery4j.transport.RedisBroker;
import io.celery4j.worker.Registry;
import io.celery4j.worker.Worker;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import redis.clients.jedis.JedisPool;

/** A Celery app in Java: `produce` sends a task, `consume` runs what arrives. */
public final class CeleryApp {

    private static final String QUEUE = "celery";

    public static void main(final String[] args) {
        final String url = System.getenv().getOrDefault("REDIS_ADDR", "redis://localhost:6379/0");
        try (
            Broker broker = new RedisBroker(new JedisPool(URI.create(url)));
            Backend backend = new RedisBackend(new JedisPool(URI.create(url)))
        ) {
            if (args.length > 0 && "consume".equals(args[0])) {
                new Worker(
                    broker,
                    backend,
                    new Registry(
                        Map.of(
                            "examples.add",
                            task -> task.args().stream().mapToInt(a -> (Integer) a).sum()
                        )
                    )
                ).some(CeleryApp.QUEUE, Duration.ofSeconds(5L), 10)
                    .forEach(done -> System.out.println(done.id() + " -> " + done.value()));
            } else {
                System.out.println(
                    "queued " + new Client(broker, backend).delay("examples.add", 2, 3).id()
                );
            }
        }
    }
}
