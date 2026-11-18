package io.github.matabar.kafkafim.supervisor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import io.github.matabar.kafkafim.KafkaTestContainers;
import io.github.matabar.kafkafim.KafkaTopics;
import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.github.matabar.kafkafim.source.kafka.KafkaPipelineSource;
import io.github.matabar.kafkafim.telemetry.PipelineMeters;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * A start that fails unexpectedly after the preflight passed leaves nothing open (#209): neither
 * the sinks it opened nor the source it created or started. Kafka only; the Elasticsearch identity
 * check is stubbed and no record is produced.
 */
@Testcontainers
class PipelineRuntimeStartFailureIntegrationTest {

  @Container static final KafkaContainer KAFKA = KafkaTestContainers.kafka();

  private static final Duration TIMEOUT = Duration.ofSeconds(60);

  private PipelineResources resources;
  private PipelineRuntime.Context context;

  @BeforeEach
  void context() throws Exception {
    String topic = "t-" + UUID.randomUUID().toString().substring(0, 8);
    try (Admin admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
      KafkaTopics.create(
          admin,
          List.of(
              new NewTopic(topic, 1, (short) 1),
              new NewTopic(topic + ".kafkafim-dlq", 1, (short) 1)));
    }
    KafkafimProperties properties =
        TestConfigs.bind(
            """
            kafkafim:
              kafka:
                clusters:
                  k: { bootstrap-servers: ["%s"] }
              elasticsearch:
                clusters:
                  es: { uris: ["http://127.0.0.1:9"] }
              pipelines:
                p:
                  source: { cluster: k, topics: [%s] }
                  routes: [ { default: true, cluster: es, index: a } ]
            """
                .formatted(KAFKA.getBootstrapServers(), topic));
    resources =
        new PipelineResources(
            new ListenerRelay(List.of(), List.of()),
            p -> (cluster, identity) -> Preflight.Status.OK,
            PipelineSupervisorTest.SETTINGS);
    context = resources.acquire("p", properties);
  }

  @AfterEach
  void close() {
    resources.close();
  }

  private void awaitFailed(PipelineRuntime runtime) {
    await().atMost(TIMEOUT).until(() -> runtime.state() == PipelineState.FAILED_PREFLIGHT);
    assertThat(runtime.failure()).contains("boom");
    assertThat(runtime.holdsResources()).isFalse();
    assertThat(runtime.source()).isNull();
  }

  /** The sinks are open when the meters are looked up; nothing has published them yet. */
  @Test
  void unexpectedExceptionAfterTheSinksOpenedClosesThem() {
    Supplier<PipelineMeters> boom =
        () -> {
          throw new IllegalStateException("boom");
        };
    PipelineRuntime runtime = new PipelineRuntime("p", context, r -> {}, boom);

    runtime.start();

    awaitFailed(runtime);
    // Never published to the runtime, so only the registry knows whether they were closed.
    assertThat(context.sinks().get("es").openSinks()).isZero();
    assertThat(runtime.retryIfFailed()).isTrue();
    runtime.stop();
  }

  /** The source is started when the state change to {@code running} is reported. */
  @Test
  void unexpectedExceptionWhileReportingRunningStopsTheSourceAndSinks() {
    AtomicReference<KafkaPipelineSource> started = new AtomicReference<>();
    Consumer<PipelineRuntime> boom =
        r -> {
          if (r.state() == PipelineState.RUNNING) {
            started.set(r.source()); // the source of the failed start, just before it is dropped
            throw new IllegalStateException("boom");
          }
        };
    PipelineRuntime runtime = new PipelineRuntime("p", context, boom);
    runtime.start();

    awaitFailed(runtime);
    assertThat(started.get()).isNotNull();
    await().atMost(TIMEOUT).until(() -> !started.get().isRunning());
    assertThat(context.sinks().get("es").openSinks()).isZero();
    runtime.stop();
  }

  /**
   * Makes the next {@code stop()} of the source throw, as a failing container stop would. Returns
   * the real container, which the test stops itself.
   */
  @SuppressWarnings("unchecked")
  private static ConcurrentMessageListenerContainer<byte[], byte[]> breakStop(
      KafkaPipelineSource source) throws ReflectiveOperationException {
    java.lang.reflect.Field field = KafkaPipelineSource.class.getDeclaredField("container");
    field.setAccessible(true);
    ConcurrentMessageListenerContainer<byte[], byte[]> real =
        (ConcurrentMessageListenerContainer<byte[], byte[]>) field.get(source);
    ConcurrentMessageListenerContainer<byte[], byte[]> broken =
        mock(ConcurrentMessageListenerContainer.class);
    doThrow(new IllegalStateException("stop failed")).when(broken).stop();
    field.set(source, broken);
    return real;
  }

  /** A source whose stop throws must not leave the sinks open and unreachable (#393). */
  @Test
  void stopClosesTheSinksWhenTheSourceStopThrows() throws Exception {
    PipelineRuntime runtime = new PipelineRuntime("p", context, r -> {});
    runtime.start();
    await().atMost(TIMEOUT).until(() -> runtime.state() == PipelineState.RUNNING);
    ConcurrentMessageListenerContainer<byte[], byte[]> real = breakStop(runtime.source());

    try {
      assertThatThrownBy(runtime::stop)
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("stop failed");

      assertThat(context.sinks().get("es").openSinks()).isZero();
      assertThat(runtime.state()).isEqualTo(PipelineState.STOPPED);
    } finally {
      real.stop();
    }
  }

  /** The same for the source of a start that failed after it began (#393). */
  @Test
  void abandonedStartClosesTheSinksWhenTheSourceStopThrows() {
    AtomicReference<ConcurrentMessageListenerContainer<byte[], byte[]>> real =
        new AtomicReference<>();
    Consumer<PipelineRuntime> boom =
        r -> {
          if (r.state() == PipelineState.RUNNING) {
            try {
              real.set(breakStop(r.source()));
            } catch (ReflectiveOperationException e) {
              throw new IllegalStateException(e);
            }
            throw new IllegalStateException("boom");
          }
        };
    PipelineRuntime runtime = new PipelineRuntime("p", context, boom);
    runtime.start();

    try {
      awaitFailed(runtime);
      assertThat(context.sinks().get("es").openSinks()).isZero();
    } finally {
      real.get().stop();
      runtime.stop();
    }
  }

  /**
   * Runs {@code action} once, from the first state report after the source was created and
   * published and before it is started (resetting the pause reasons reports under the runtime's
   * lock), which is how these tests reach the middle of {@code attemptStart} without a seam.
   */
  private static Consumer<PipelineRuntime> betweenCreateAndStart(
      AtomicReference<KafkaPipelineSource> created, Consumer<PipelineRuntime> action) {
    AtomicBoolean fired = new AtomicBoolean();
    return r -> {
      KafkaPipelineSource source = r.source();
      if (source != null
          && r.state() == PipelineState.STARTING
          && fired.compareAndSet(false, true)) {
        created.set(source);
        action.accept(r);
      }
    };
  }

  /** The source reports its failure while the start is still going on (ADR-0036, #438). */
  @Test
  void sourceFailedWhileTheSourceIsStartingFailsThePipelineAndClosesEverything() {
    AtomicReference<KafkaPipelineSource> created = new AtomicReference<>();
    PipelineRuntime runtime =
        new PipelineRuntime(
            "p",
            context,
            betweenCreateAndStart(created, r -> r.sourceFailed(created.get(), "consumer died")));

    runtime.start();

    await().atMost(TIMEOUT).until(() -> runtime.state() == PipelineState.FAILED);
    await().atMost(TIMEOUT).until(() -> !runtime.holdsResources());
    // The start that was under way did not turn the failed pipeline into a running one.
    assertThat(runtime.state()).isEqualTo(PipelineState.FAILED);
    assertThat(runtime.failure()).isEqualTo("consumer died");
    assertThat(context.sinks().get("es").openSinks()).isZero();
    await().atMost(TIMEOUT).until(() -> !created.get().isRunning());
    // Nothing is stopping it any more: it can be retried, and a stop then ends that start.
    await().atMost(TIMEOUT).until(runtime::retryIfFailed);
    runtime.stop();
    assertThat(runtime.state()).isEqualTo(PipelineState.STOPPED);
  }

  /**
   * A stop requested after the sinks opened and the source was created, before it started: the
   * start gives up, and the stop closes the source and the sinks (#438). Which side wins the last
   * check is a race the test cannot pin without a seam; either way nothing may stay open.
   */
  @Test
  void stopBetweenOpeningTheSinksAndStartingTheSourceLeavesNothingOpen() throws Exception {
    AtomicReference<KafkaPipelineSource> created = new AtomicReference<>();
    AtomicReference<Thread> stopper = new AtomicReference<>();
    PipelineRuntime runtime =
        new PipelineRuntime(
            "p",
            context,
            betweenCreateAndStart(
                created,
                r ->
                    // Blocks on the runtime's lock, held here, until this report returns.
                    stopper.set(Thread.ofPlatform().name("test-stop").start(r::stop))));

    runtime.start();
    await().atMost(TIMEOUT).until(() -> stopper.get() != null);
    stopper.get().join(TIMEOUT);

    assertThat(stopper.get().isAlive()).isFalse();
    assertThat(runtime.state()).isEqualTo(PipelineState.STOPPED);
    assertThat(runtime.holdsResources()).isFalse();
    assertThat(context.sinks().get("es").openSinks()).isZero();
    assertThat(created.get().isRunning()).isFalse();
    // The stop counter is back to zero: a start is accepted again.
    runtime.start();
    runtime.stop();
  }
}
