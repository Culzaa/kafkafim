package io.github.matabar.kafkafim.telemetry;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Meter binding of DESIGN §10 against a {@link SimpleMeterRegistry}, and the §10 names. */
class KafkafimMetersTest {

  private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
  private final KafkafimMeters meters = new KafkafimMeters(registry);
  private final Map<String, CircuitBreaker.State> breakers = new HashMap<>();

  @AfterEach
  void close() {
    meters.close();
  }

  /** A probe with settable values. */
  static final class Probe implements SourceProbe {
    final AtomicLong inFlight = new AtomicLong();
    final AtomicInteger assigned = new AtomicInteger();
    volatile boolean connected;
    volatile Set<PauseCause> pauseReasons = Set.of();

    @Override
    public long inFlightBytes() {
      return inFlight.get();
    }

    @Override
    public int assignedPartitions() {
      return assigned.get();
    }

    @Override
    public boolean connected() {
      return connected;
    }

    @Override
    public Set<PauseCause> pauseReasons() {
      return pauseReasons;
    }
  }

  private static PipelineSpec spec(String kafka, List<String> topics, SourceProbe probe) {
    return new PipelineSpec(kafka, topics, List.of("eu", "1"), Set.of("search-eu"), probe);
  }

  private void sync(Map<String, PipelineSpec> specs) {
    Set<String> clusters =
        specs.values().stream()
            .flatMap(s -> s.esClusters().stream())
            .collect(Collectors.toCollection(LinkedHashSet::new));
    meters.sync(specs, clusters, breakers::get);
  }

  private double counter(String name, String... tags) {
    return registry.get(name).tags(tags).counter().count();
  }

  private double gauge(String name, String... tags) {
    return registry.get(name).tags(tags).gauge().value();
  }

  private Set<String> pipelinesOf(String name) {
    return registry.find(name).meters().stream()
        .map(m -> m.getId().getTag("pipeline"))
        .collect(Collectors.toSet());
  }

  @Test
  void configuredSeriesExistFromTheStartWithBothBaseLabels() {
    Probe probe = new Probe();
    sync(Map.of("orders", spec("prod-a", List.of("orders.v1", "orders.v2"), probe)));

    for (String topic : List.of("orders.v1", "orders.v2")) {
      String[] tags = {"pipeline", "orders", "kafka_cluster", "prod-a", "topic", topic};
      assertThat(counter(TelemetryNames.RECORDS_CONSUMED, tags)).isZero();
      assertThat(counter(TelemetryNames.BYTES_CONSUMED, tags)).isZero();
    }
    for (String route : List.of("eu", "1")) {
      for (String outcome : List.of("indexed", "stale", "dlq")) {
        assertThat(
                counter(
                    TelemetryNames.DOCS,
                    "pipeline",
                    "orders",
                    "kafka_cluster",
                    "prod-a",
                    "route",
                    route,
                    "outcome",
                    outcome))
            .isZero();
      }
    }
    // Pre-routing failures (route="none") can only end in the DLQ.
    assertThat(
            counter(
                TelemetryNames.DOCS,
                "pipeline",
                "orders",
                "kafka_cluster",
                "prod-a",
                "route",
                TelemetryNames.NO_ROUTE,
                "outcome",
                "dlq"))
        .isZero();
    assertThat(
            registry
                .find(TelemetryNames.DOCS)
                .tag("route", TelemetryNames.NO_ROUTE)
                .meters()
                .stream()
                .map(m -> m.getId().getTag("outcome"))
                .toList())
        .containsExactly("dlq");
    assertThat(registry.get(TelemetryNames.BULK_LATENCY).tag("es_cluster", "search-eu").timer())
        .isNotNull();
    assertThat(registry.get(TelemetryNames.E2E_LATENCY).tag("pipeline", "orders").timer())
        .isNotNull();
    assertThat(gauge(TelemetryNames.BREAKER_STATE, "es_cluster", "search-eu")).isZero();
    // Every pipeline series has pipeline and kafka_cluster.
    for (Meter meter : registry.getMeters()) {
      if (!meter.getId().getName().equals(TelemetryNames.BREAKER_STATE)) {
        assertThat(meter.getId().getTags())
            .extracting(Tag::getKey)
            .as(meter.getId().toString())
            .contains("pipeline", "kafka_cluster")
            .doesNotContain("partition", "index");
      }
    }
  }

  @Test
  void eventsLandOnTheirSeries() {
    sync(Map.of("orders", spec("prod-a", List.of("orders.v1"), new Probe())));
    PipelineMeters orders = meters.pipeline("orders");

    orders.consumed("orders.v1", 120);
    orders.consumed("orders.v1", 30);
    orders.document("eu", DocOutcome.INDEXED);
    orders.document(TelemetryNames.NO_ROUTE, DocOutcome.DLQ);
    orders.endToEnd(Duration.ofMillis(250));
    orders.endToEnd(Duration.ofMillis(-5));
    orders.flush("search-eu", "DOCS");
    orders.bulkResponse("search-eu", 200, 10, 4096, Duration.ofMillis(40));
    orders.bulkResponse("search-eu", 429, 10, 4096, Duration.ofMillis(5));
    orders.bulkTransportError("search-eu");
    orders.itemError("search-eu", "mapper_parsing_exception", false);
    orders.itemError("search-eu", "es_rejected_execution_exception", true);
    orders.retry("search-eu", "request");
    orders.retry("search-eu", "item");
    orders.authFailure("search-eu", "writer");
    orders.authFailure("search-eu", null);
    orders.dlqRecord("es_error");
    orders.routeRegexBudgetExceeded();
    orders.routeRegexBudgetExceeded();

    String[] base = {"pipeline", "orders", "kafka_cluster", "prod-a"};
    assertThat(counter(TelemetryNames.RECORDS_CONSUMED, with(base, "topic", "orders.v1")))
        .isEqualTo(2);
    assertThat(counter(TelemetryNames.BYTES_CONSUMED, with(base, "topic", "orders.v1")))
        .isEqualTo(150);
    assertThat(counter(TelemetryNames.DOCS, with(base, "route", "eu", "outcome", "indexed")))
        .isOne();
    assertThat(counter(TelemetryNames.DOCS, with(base, "route", "none", "outcome", "dlq"))).isOne();
    assertThat(counter(TelemetryNames.ROUTE_REGEX_BUDGET_EXCEEDED, base)).isEqualTo(2);
    assertThat(registry.get(TelemetryNames.E2E_LATENCY).tags(base).timer().count()).isEqualTo(2);
    assertThat(
            registry.get(TelemetryNames.E2E_LATENCY).tags(base).timer().max(TimeUnit.MILLISECONDS))
        .isEqualTo(250);
    assertThat(
            counter(
                TelemetryNames.BATCH_FLUSH,
                with(base, "es_cluster", "search-eu", "reason", "docs")))
        .isOne();
    assertThat(
            counter(
                TelemetryNames.BULK_REQUESTS,
                with(base, "es_cluster", "search-eu", "status", "200")))
        .isOne();
    assertThat(
            counter(
                TelemetryNames.BULK_REQUESTS,
                with(base, "es_cluster", "search-eu", "status", "429")))
        .isOne();
    assertThat(
            counter(
                TelemetryNames.BULK_REQUESTS,
                with(base, "es_cluster", "search-eu", "status", "io_error")))
        .isOne();
    assertThat(registry.get(TelemetryNames.BULK_DOCS).tags(base).summary().totalAmount())
        .isEqualTo(20);
    assertThat(registry.get(TelemetryNames.BULK_BYTES).tags(base).summary().totalAmount())
        .isEqualTo(8192);
    assertThat(registry.get(TelemetryNames.BULK_LATENCY).tags(base).timer().count()).isEqualTo(2);
    assertThat(
            counter(
                TelemetryNames.ITEM_ERRORS,
                with(
                    base,
                    "es_cluster",
                    "search-eu",
                    "error_type",
                    "mapper_parsing_exception",
                    "retryable",
                    "false")))
        .isOne();
    assertThat(
            counter(
                TelemetryNames.ITEM_ERRORS,
                with(
                    base,
                    "es_cluster",
                    "search-eu",
                    "error_type",
                    "es_rejected_execution_exception",
                    "retryable",
                    "true")))
        .isOne();
    assertThat(
            counter(
                TelemetryNames.RETRIES, with(base, "es_cluster", "search-eu", "scope", "request")))
        .isOne();
    assertThat(
            counter(
                TelemetryNames.AUTH_FAILURES,
                with(base, "es_cluster", "search-eu", "identity", "writer")))
        .isOne();
    assertThat(
            counter(
                TelemetryNames.AUTH_FAILURES,
                with(base, "es_cluster", "search-eu", "identity", "none")))
        .isOne();
    assertThat(counter(TelemetryNames.DLQ_RECORDS, with(base, "error_type", "es_error"))).isOne();
  }

  @Test
  void authFailureSeriesExistFromTheStartPerClusterAndIdentity() {
    Set<PipelineSpec.EsStream> streams =
        Set.of(
            new PipelineSpec.EsStream("search-eu", "writer"),
            new PipelineSpec.EsStream("search-eu", "reader"),
            new PipelineSpec.EsStream("plain", null));
    sync(
        Map.of(
            "orders",
            new PipelineSpec(
                "prod-a",
                List.of("t"),
                List.of("0"),
                Set.of("search-eu", "plain"),
                streams,
                new Probe())));
    String[] base = {"pipeline", "orders", "kafka_cluster", "prod-a"};

    assertThat(
            registry.find(TelemetryNames.AUTH_FAILURES).counters().stream()
                .map(c -> c.getId().getTag("es_cluster") + "/" + c.getId().getTag("identity"))
                .toList())
        .containsExactlyInAnyOrder("search-eu/writer", "search-eu/reader", "plain/none");
    assertThat(
            counter(
                TelemetryNames.AUTH_FAILURES,
                with(base, "es_cluster", "search-eu", "identity", "writer")))
        .isZero();

    meters.pipeline("orders").authFailure("search-eu", "writer");
    meters.pipeline("orders").authFailure("plain", null);
    assertThat(
            counter(
                TelemetryNames.AUTH_FAILURES,
                with(base, "es_cluster", "search-eu", "identity", "writer")))
        .isOne();
    assertThat(
            counter(
                TelemetryNames.AUTH_FAILURES,
                with(base, "es_cluster", "plain", "identity", "none")))
        .isOne();
    assertThat(registry.find(TelemetryNames.AUTH_FAILURES).counters()).hasSize(3);
  }

  @Test
  void changedIdentityGetsFreshAuthFailureSeries() {
    sync(Map.of("orders", withStream("writer")));
    sync(Map.of("orders", withStream("writer-v2")));

    assertThat(
            registry.find(TelemetryNames.AUTH_FAILURES).counters().stream()
                .map(c -> c.getId().getTag("identity"))
                .toList())
        .containsExactly("writer-v2");
  }

  private static PipelineSpec withStream(String identity) {
    return new PipelineSpec(
        "prod-a",
        List.of("t"),
        List.of("0"),
        Set.of("search-eu"),
        Set.of(new PipelineSpec.EsStream("search-eu", identity)),
        new Probe());
  }

  @Test
  void gaugesReadTheProbe() {
    Probe probe = new Probe();
    sync(Map.of("orders", spec("prod-a", List.of("orders.v1", "orders.v2"), probe)));
    probe.inFlight.set(2048);
    probe.assigned.set(3);
    probe.connected = true;
    final String[] base = {"pipeline", "orders", "kafka_cluster", "prod-a"};
    assertThat(gauge(TelemetryNames.IN_FLIGHT_BYTES, base)).isEqualTo(2048);
    assertThat(gauge(TelemetryNames.ASSIGNED_PARTITIONS, base)).isEqualTo(3);
    assertThat(gauge(TelemetryNames.KAFKA_CONNECTED, base)).isOne();
  }

  @Test
  void pauseReasonGaugesExistPerConfiguredCauseAndReadTheProbe() {
    Probe probe = new Probe();
    sync(
        Map.of(
            "orders",
            new PipelineSpec(
                "prod-a",
                List.of("t"),
                List.of("0"),
                Set.of("es", "es2"),
                Set.of(
                    new PipelineSpec.EsStream("es", "writer"),
                    new PipelineSpec.EsStream("es2", null)),
                probe)));

    assertThat(
            registry.find(TelemetryNames.PAUSE_REASON).gauges().stream()
                .map(g ->
                    g.getId().getTag("reason")
                        + "/"
                        + g.getId().getTag("es_cluster")
                        + "/"
                        + g.getId().getTag("identity"))
                .toList())
        .containsExactlyInAnyOrder(
            "es_breaker/es/none",
            "es_breaker/es2/none",
            "es_auth/es/writer",
            "es_auth/es2/none",
            "dlq_blocked/none/none");
    assertThat(pauseGauge("es_breaker", "es", "none")).isZero();

    probe.pauseReasons = Set.of(PauseCause.breaker("es"), PauseCause.dlqBlocked());
    assertThat(pauseGauge("es_breaker", "es", "none")).isOne();
    assertThat(pauseGauge("es_breaker", "es2", "none")).isZero();
    assertThat(pauseGauge("dlq_blocked", "none", "none")).isOne();
    assertThat(pauseGauge("es_auth", "es", "writer")).isZero();

    probe.pauseReasons = Set.of(PauseCause.auth("es2", null));
    assertThat(pauseGauge("es_auth", "es2", "none")).isOne();
    assertThat(pauseGauge("es_breaker", "es", "none")).isZero();
  }

  private double pauseGauge(String reason, String esCluster, String identity) {
    return gauge(
        TelemetryNames.PAUSE_REASON,
        "pipeline",
        "orders",
        "kafka_cluster",
        "prod-a",
        "reason",
        reason,
        "es_cluster",
        esCluster,
        "identity",
        identity);
  }

  @Test
  void breakerGaugeEncodesTheState() {
    sync(Map.of("orders", spec("prod-a", List.of("t"), new Probe())));

    breakers.put("search-eu", CircuitBreaker.State.OPEN);
    assertThat(gauge(TelemetryNames.BREAKER_STATE, "es_cluster", "search-eu")).isEqualTo(2);
    breakers.put("search-eu", CircuitBreaker.State.HALF_OPEN);
    assertThat(gauge(TelemetryNames.BREAKER_STATE, "es_cluster", "search-eu")).isOne();
    breakers.put("search-eu", CircuitBreaker.State.CLOSED);
    assertThat(gauge(TelemetryNames.BREAKER_STATE, "es_cluster", "search-eu")).isZero();
  }

  @Test
  void reloadRemovesSeriesOfRemovedPipelinesAndAddsThoseOfNewOnes() {
    Map<String, PipelineSpec> first = new LinkedHashMap<>();
    first.put("orders", spec("prod-a", List.of("orders.v1"), new Probe()));
    first.put("audit", spec("prod-a", List.of("audit"), new Probe()));
    sync(first);
    meters.pipeline("orders").consumed("orders.v1", 10);
    final PipelineMeters audit = meters.pipeline("audit");

    Map<String, PipelineSpec> second = new LinkedHashMap<>();
    second.put("orders", spec("prod-a", List.of("orders.v1"), new Probe()));
    second.put("billing", spec("prod-b", List.of("billing"), new Probe()));
    sync(second);

    assertThat(pipelinesOf(TelemetryNames.RECORDS_CONSUMED)).containsOnly("orders", "billing");
    assertThat(pipelinesOf(TelemetryNames.IN_FLIGHT_BYTES)).containsOnly("orders", "billing");
    // An unchanged pipeline keeps its counters across the reload.
    assertThat(
            counter(
                TelemetryNames.RECORDS_CONSUMED,
                "pipeline",
                "orders",
                "kafka_cluster",
                "prod-a",
                "topic",
                "orders.v1"))
        .isEqualTo(1);
    // Late events of the removed pipeline create nothing.
    audit.consumed("audit", 5);
    audit.dlqRecord("es_error");
    assertThat(meters.pipeline("audit")).isSameAs(PipelineMeters.NOOP);
    assertThat(pipelinesOf(TelemetryNames.DLQ_RECORDS)).doesNotContain("audit");
    assertThat(pipelinesOf(TelemetryNames.RECORDS_CONSUMED)).doesNotContain("audit");
  }

  @Test
  void relabelledPipelineGetsFreshSeriesAndLosesTheOldOnes() {
    sync(Map.of("orders", spec("prod-a", List.of("orders.v1"), new Probe())));
    sync(Map.of("orders", spec("prod-b", List.of("orders.v1"), new Probe())));

    assertThat(
            registry.find(TelemetryNames.RECORDS_CONSUMED).meters().stream()
                .map(m -> m.getId().getTag("kafka_cluster"))
                .toList())
        .containsExactly("prod-b");
  }

  @Test
  void breakerSeriesFollowTheUsedClusters() {
    sync(Map.of("orders", spec("prod-a", List.of("t"), new Probe())));
    meters.sync(Map.of(), Set.of(), breakers::get);

    assertThat(registry.find(TelemetryNames.BREAKER_STATE).meters()).isEmpty();
    assertThat(registry.getMeters()).isEmpty();
  }

  @Test
  void noopMetersRecordNothing() {
    PipelineMeters.NOOP.consumed("t", 1);
    PipelineMeters.NOOP.document("r", DocOutcome.INDEXED);
    PipelineMeters.NOOP.bulkResponse("c", 200, 1, 1, Duration.ZERO);
    assertThat(registry.getMeters()).isEmpty();
    assertThat(meters.pipeline("unknown")).isSameAs(PipelineMeters.NOOP);
  }

  /** The §10 meter names and tags, as Micrometer registers them (the OTLP export sends these). */
  @Test
  void registersTheDesignNamesAndTags() {
    SimpleMeterRegistry simple = new SimpleMeterRegistry();
    try (KafkafimMeters onSimple = new KafkafimMeters(simple)) {
      onSimple.sync(
          Map.of("orders", spec("prod-a", List.of("orders.v1"), new Probe())),
          Set.of("search-eu"),
          c -> CircuitBreaker.State.CLOSED);
      PipelineMeters orders = onSimple.pipeline("orders");
      orders.flush("search-eu", "AGE");
      orders.bulkResponse("search-eu", 200, 3, 300, Duration.ofMillis(3));
      orders.itemError("search-eu", "mapper_parsing_exception", false);
      orders.retry("search-eu", "request");
      orders.authFailure("search-eu", "writer");
      orders.dlqRecord("decode");

      String[] base = {"kafka_cluster", "prod-a", "pipeline", "orders"};
      assertThat(
              simple
                  .find("kafkafim.records.consumed")
                  .tags(with(base, "topic", "orders.v1"))
                  .counter())
          .isNotNull();
      assertThat(simple.find("kafkafim.bytes.consumed").tags(base).counter()).isNotNull();
      assertThat(
              simple
                  .find("kafkafim.docs")
                  .tags(with(base, "outcome", "indexed", "route", "eu"))
                  .counter())
          .isNotNull();
      assertThat(
              simple
                  .find("kafkafim.bulk.requests")
                  .tags(with(base, "es_cluster", "search-eu", "status", "200"))
                  .counter()
                  .count())
          .isEqualTo(1);
      assertThat(simple.find("kafkafim.bulk.latency").tags(base).timer().count()).isEqualTo(1);
      assertThat(simple.find("kafkafim.bulk.docs").tags(base).summary().totalAmount()).isEqualTo(3);
      assertThat(simple.find("kafkafim.bulk.bytes").tags(base).summary().totalAmount())
          .isEqualTo(300);
      assertThat(
              simple
                  .find("kafkafim.item.errors")
                  .tags(
                      with(
                          base,
                          "error_type",
                          "mapper_parsing_exception",
                          "es_cluster",
                          "search-eu",
                          "retryable",
                          "false"))
                  .counter()
                  .count())
          .isEqualTo(1);
      assertThat(simple.find("kafkafim.retries").tags(base).counter()).isNotNull();
      assertThat(
              simple.find("kafkafim.breaker.state").tag("es_cluster", "search-eu").gauge().value())
          .isZero();
      assertThat(
              simple
                  .find("kafkafim.auth.failures")
                  .tags(with(base, "es_cluster", "search-eu", "identity", "writer"))
                  .counter())
          .isNotNull();
      assertThat(simple.find("kafkafim.e2e.latency").tags(base).timer()).isNotNull();
      assertThat(
              simple
                  .find("kafkafim.batch.flush")
                  .tags(with(base, "es_cluster", "search-eu", "reason", "age"))
                  .counter()
                  .count())
          .isEqualTo(1);
      assertThat(simple.find("kafkafim.in.flight.bytes").tags(base).gauge()).isNotNull();
      assertThat(simple.find("kafkafim.assigned.partitions").tags(base).gauge()).isNotNull();
      assertThat(simple.find("kafkafim.kafka.connected").tags(base).gauge()).isNotNull();
      assertThat(
              simple
                  .find("kafkafim.pipeline.pause.reason")
                  .tags(with(base, "reason", "dlq_blocked"))
                  .gauge())
          .isNotNull();
      assertThat(
              simple
                  .find("kafkafim.dlq.records")
                  .tags(with(base, "error_type", "decode"))
                  .counter()
                  .count())
          .isEqualTo(1);
      assertThat(simple.getMeters().stream().map(m -> m.getId().getName()))
          .noneMatch(name -> name.contains("consumer.lag"));
    } finally {
      simple.close();
    }
  }

  private static String[] with(String[] base, String... more) {
    String[] all = new String[base.length + more.length];
    System.arraycopy(base, 0, all, 0, base.length);
    System.arraycopy(more, 0, all, base.length, more.length);
    return all;
  }
}
