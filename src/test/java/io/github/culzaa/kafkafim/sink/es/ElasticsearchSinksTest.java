package io.github.culzaa.kafkafim.sink.es;

import static io.github.culzaa.kafkafim.sink.es.SinkHarness.doc;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.github.culzaa.kafkafim.config.ElasticsearchCluster;
import io.github.culzaa.kafkafim.config.Pipeline.OnVersionConflict;
import io.github.culzaa.kafkafim.sink.es.SinkHarness.Events;
import io.github.culzaa.kafkafim.sink.es.SinkHarness.Outcomes;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/** The per-cluster {@code client} block reaches the clients and the breaker (ADR-0031). */
class ElasticsearchSinksTest {

  static ElasticsearchCluster cluster(String uri, ElasticsearchCluster.@Nullable Client client) {
    return new ElasticsearchCluster(List.of(uri), null, Map.of(), null, null, false, client);
  }

  @Test
  void breakerListenerErrorIsContained() {
    AtomicInteger calls = new AtomicInteger();
    SinkListener failing =
        new SinkListener() {
          @Override
          public void onBreakerStateChange(
              String cluster, CircuitBreaker.State from, CircuitBreaker.State to) {
            calls.incrementAndGet();
            throw new LinkageError("listener bug");
          }
        };
    // The breaker library swallows a throwing consumer itself, so the proof that the sink's own
    // guard ran is its log line.
    Logger logger = (Logger) LoggerFactory.getLogger(ElasticsearchSinks.class);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    try (ElasticsearchSinks sinks =
        new ElasticsearchSinks(
            Map.of("c", cluster("http://es-a:9200", null)),
            ClientSettings.DEFAULT,
            BreakerSettings.DEFAULT,
            failing)) {
      CircuitBreaker breaker = sinks.breaker("c");

      breaker.transitionToOpenState();

      assertThat(calls).hasValue(1);
      assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
      assertThat(appender.list)
          .anyMatch(e ->
              e.getFormattedMessage().equals("Sink listener failed")
                  && e.getThrowableProxy() != null);
    } finally {
      logger.detachAppender(appender);
    }
  }

  @Test
  void theHalfOpenLimitFollowsEachClustersResponseTimeout() {
    Map<String, ElasticsearchCluster> clusters =
        Map.of(
            "defaults", cluster("http://es-a:9200", null),
            "fast",
                cluster(
                    "http://es-b:9200",
                    new ElasticsearchCluster.Client(null, Duration.ofSeconds(5))));
    try (ElasticsearchSinks sinks =
        new ElasticsearchSinks(
            clusters, ClientSettings.DEFAULT, BreakerSettings.DEFAULT, new Events())) {
      // waitInOpen (10s) plus twice the response timeout; the default is the former fixed 2min.
      assertThat(
              sinks
                  .breaker("defaults")
                  .getCircuitBreakerConfig()
                  .getMaxWaitDurationInHalfOpenState())
          .isEqualTo(Duration.ofSeconds(130));
      assertThat(
              sinks.breaker("fast").getCircuitBreakerConfig().getMaxWaitDurationInHalfOpenState())
          .isEqualTo(Duration.ofSeconds(20));
    }
  }

  @Test
  void theClustersResponseTimeoutAppliesToItsClients() throws Exception {
    // The kernel completes the handshake into the backlog, the request is sent, and no answer
    // ever comes: only the response timeout ends the wait.
    try (ServerSocket silent = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
      ElasticsearchCluster cluster =
          cluster(
              "http://127.0.0.1:" + silent.getLocalPort(),
              new ElasticsearchCluster.Client(Duration.ofSeconds(1), Duration.ofMillis(300)));
      Events events = new Events();
      try (ElasticsearchSinks sinks =
          new ElasticsearchSinks(
              Map.of("silent", cluster), ClientSettings.DEFAULT, BreakerSettings.DEFAULT, events)) {
        BulkSink<String> sink =
            sinks.open(
                new SinkKey("p", "silent", null),
                new SinkSettings(
                    1,
                    1 << 20,
                    Duration.ofMillis(10),
                    1,
                    1,
                    Duration.ZERO,
                    OnVersionConflict.IGNORE,
                    new Backoff(Duration.ofMillis(50), Duration.ofMillis(100), 2.0, 0.0)),
                new Outcomes());
        long start = System.nanoTime();
        sink.submit(doc("i", "1", "{}"));

        // With the 60s default no retry would be reported within the wait.
        await().atMost(Duration.ofSeconds(10)).until(() -> events.requestRetries.get() >= 2);
        // Each attempt waited for the timeout: not an immediate connection error.
        assertThat(Duration.ofNanos(System.nanoTime() - start))
            .isGreaterThanOrEqualTo(Duration.ofMillis(600));
        sink.close(Duration.ZERO);
      }
    }
  }

  @Test
  void forClusterTakesTheTimeoutsAndKeepsThePool() {
    ClientSettings settings =
        ClientSettings.DEFAULT.forCluster(
            new ElasticsearchCluster.Client(Duration.ofSeconds(2), Duration.ofSeconds(90)));
    assertThat(settings)
        .isEqualTo(
            new ClientSettings(
                Duration.ofSeconds(2),
                Duration.ofSeconds(90),
                ClientSettings.DEFAULT.connectionRequestTimeout(),
                ClientSettings.DEFAULT.maxConnectionsPerNode(),
                ClientSettings.DEFAULT.maxConnectionsTotal()));
  }

  @Test
  void theConfigDefaultsMatchTheClientDefaults() {
    ElasticsearchCluster.Client defaults = new ElasticsearchCluster.Client(null, null);
    assertThat(ClientSettings.DEFAULT.forCluster(defaults)).isEqualTo(ClientSettings.DEFAULT);
  }
}
