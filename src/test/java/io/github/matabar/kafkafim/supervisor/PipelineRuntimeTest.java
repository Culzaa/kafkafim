package io.github.matabar.kafkafim.supervisor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * One {@link PipelineRuntime} without a broker: its pause reasons against a change of its building
 * blocks (events of the DLQ producers and registries a reload replaced), and a start that fails
 * unexpectedly.
 */
class PipelineRuntimeTest {

  private static final String A =
      """
      a:
        source: { cluster: k, topics: [a] }
        routes: [ { default: true, cluster: es, index: a } ]
      """;

  private final PipelineResources resources =
      new PipelineResources(
          new ListenerRelay(List.of(), List.of()),
          p -> (cluster, identity) -> Preflight.Status.OK,
          PipelineSupervisorTest.SETTINGS);

  /** The context of the startup configuration. */
  private final PipelineRuntime.Context first = resources.acquire("a", TestConfigs.bind(yaml(A)));

  /** A context with other DLQ producers (the Kafka cluster changed) and another es registry. */
  private final PipelineRuntime.Context second =
      resources.acquire(
          "a",
          TestConfigs.bind(
              yaml(A).replace("k1:9092", "k2:9092").replace("password: p", "password: p2")));

  private final PipelineRuntime runtime = new PipelineRuntime("a", first, r -> {});

  @AfterEach
  void close() {
    resources.close();
  }

  private static String yaml(String pipelines) {
    return TestConfigs.CLUSTERS + "  pipelines:\n" + pipelines.indent(4);
  }

  private void dlqEvent(PipelineRuntime.Context from, boolean blocked) {
    runtime.pauseIfOwner(
        r -> r.usesDlq(from.dlqProducers()), PipelineRuntime.DLQ_BLOCKED_REASON, blocked);
  }

  private void breakerEvent(PipelineRuntime.Context from, boolean open) {
    runtime.pauseIfOwner(
        r -> r.usesSinks("es", from.sinks().get("es")),
        PipelineRuntime.BREAKER_REASON_PREFIX + "es",
        open);
  }

  @Test
  void sourceConcurrencyOfThePipelineOverridesTheCpuDefaultOnly() {
    var unset = TestConfigs.bind(yaml(A)).pipelines().get("a");
    var set =
        TestConfigs.bind(yaml(A.replace("topics: [a]", "topics: [a], concurrency: 5")))
            .pipelines()
            .get("a");

    assertThat(runtime.sourceOptions(unset).desiredConcurrency())
        .isEqualTo(Runtime.getRuntime().availableProcessors());
    assertThat(runtime.sourceOptions(set).desiredConcurrency()).isEqualTo(5);
    assertThat(runtime.sourceOptions(set).drainTimeout())
        .isEqualTo(runtime.sourceOptions(unset).drainTimeout());
  }

  /**
   * The race {@link PipelineRuntime#pauseIfOwner} closes: an old DLQ writer's {@code blocked} event
   * passes the ownership check, the reload reconfigures the pipeline (dropping {@code dlq-blocked}
   * with the old writer), and only then the pause lands, with nothing left to clear it. Check and
   * pause are one step under the runtime's lock, so the reconfigure has to wait for both.
   */
  @Test
  void reconfigureWaitsForAnEventThatPassedTheOwnershipCheck() throws Exception {
    AtomicReference<Thread.State> duringEvent = new AtomicReference<>();
    Thread reconfigurer =
        Thread.ofPlatform().name("test-reconfigure").unstarted(() -> runtime.reconfigure(second));

    runtime.pauseIfOwner(
        r -> {
          final boolean owns = r.usesDlq(first.dlqProducers());
          // The reload gets its chance exactly between the ownership check and the pause.
          reconfigurer.start();
          await()
              .atMost(Duration.ofSeconds(10))
              .until(() ->
                  reconfigurer.getState() == Thread.State.BLOCKED
                      || reconfigurer.getState() == Thread.State.TERMINATED);
          duringEvent.set(reconfigurer.getState());
          return owns;
        },
        PipelineRuntime.DLQ_BLOCKED_REASON,
        true);
    reconfigurer.join(Duration.ofSeconds(10));

    assertThat(duringEvent.get()).isEqualTo(Thread.State.BLOCKED);
    assertThat(reconfigurer.isAlive()).isFalse();
    assertThat(runtime.context()).isSameAs(second);
    // The pause landed first, so the reconfigure dropped it with the old writer.
    assertThat(runtime.pauseReasons()).isEmpty();
  }

  @Test
  void dlqEventsOfTheWriterLeftBehindNeitherPauseNorResume() {
    assertThat(second.dlqProducers()).isNotSameAs(first.dlqProducers());
    dlqEvent(first, true);
    assertThat(runtime.pauseReasons()).containsExactly("dlq-blocked");

    assertThat(runtime.reconfigure(second)).isSameAs(first);
    assertThat(runtime.pauseReasons()).isEmpty();
    dlqEvent(first, true);
    assertThat(runtime.pauseReasons()).isEmpty();

    // The new writer blocks: the old writer's late `unblocked` must not clear that.
    dlqEvent(second, true);
    dlqEvent(first, false);
    assertThat(runtime.pauseReasons()).containsExactly("dlq-blocked");
    dlqEvent(second, false);
    assertThat(runtime.pauseReasons()).isEmpty();
  }

  @Test
  void reconfigureOnTheSameDlqProducersKeepsDlqBlocked() {
    // Only the pipeline definition changes: same Kafka cluster, same DLQ settings, same writer.
    PipelineRuntime.Context sameWriter =
        resources.acquire(
            "a",
            TestConfigs.bind(
                yaml(A.replace("index: a", "index: a2"))
                    .replace("k1:9092", "k2:9092")
                    .replace("password: p", "password: p2")));
    assertThat(sameWriter.dlqProducers()).isSameAs(second.dlqProducers());
    runtime.reconfigure(second);
    dlqEvent(second, true);

    runtime.reconfigure(sameWriter);

    assertThat(runtime.pauseReasons()).containsExactly("dlq-blocked");
  }

  @Test
  void breakerEventsOfTheRegistryLeftBehindNeitherPauseNorResume() {
    assertThat(second.sinks().get("es")).isNotSameAs(first.sinks().get("es"));
    breakerEvent(first, true);
    assertThat(runtime.pauseReasons()).containsExactly("es-breaker:es");

    // Unlike dlq-blocked, a breaker reason outlives the reconfigure; the next start drops it.
    runtime.reconfigure(second);
    assertThat(runtime.pauseReasons()).containsExactly("es-breaker:es");
    // The old registry's breaker closing says nothing about the new one.
    breakerEvent(first, false);
    assertThat(runtime.pauseReasons()).containsExactly("es-breaker:es");
    runtime.resetReasonsForStart();
    assertThat(runtime.pauseReasons()).isEmpty();

    breakerEvent(first, true);
    assertThat(runtime.pauseReasons()).isEmpty();
    breakerEvent(second, true);
    assertThat(runtime.pauseReasons()).containsExactly("es-breaker:es");
    breakerEvent(second, false);
    assertThat(runtime.pauseReasons()).isEmpty();
  }

  @Test
  void anEventWithoutOwnerOrForAnotherClusterIsNotThisPipelines() {
    assertThat(runtime.usesSinks("es", null)).isFalse();
    assertThat(runtime.usesSinks("es2", first.sinks().get("es"))).isFalse();
    assertThat(runtime.usesSinks("es", first.sinks().get("es"))).isTrue();
    assertThat(runtime.usesDlq(null)).isFalse();
    assertThat(runtime.usesDlq(first.dlqProducers())).isTrue();
  }

  @Test
  void pauseReasonDoesNotMakeStoppedPipelinePaused() {
    dlqEvent(first, true);
    assertThat(runtime.state()).isEqualTo(PipelineState.STOPPED);
  }

  @Test
  void unexpectedExceptionWhileStartingFailsThePipelineAndLeavesItRetryable() {
    // Closed DLQ producers answer writer() with an IllegalStateException, which no check expects.
    first.dlqProducers().close();

    runtime.start();

    await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> runtime.state() == PipelineState.FAILED_PREFLIGHT);
    assertThat(runtime.failure()).contains("IllegalStateException");
    // The starter was forgotten with the state, so a retry can begin at once.
    assertThat(runtime.retryIfFailed()).isTrue();
    runtime.stop();
  }

  /**
   * An {@link Error} from the starter (here the identity check) is not a {@code RuntimeException},
   * but the pipeline must not stay {@code starting} with no starter, where {@code retryIfFailed}
   * never acts (#209). The error still escapes the starter thread, as it would anywhere.
   */
  @Test
  void errorWhileStartingFailsThePipelineAndLeavesItRetryable() {
    // The starter rethrows the error after cleaning up; keep it from failing the test run.
    AtomicReference<Throwable> escaped = new AtomicReference<>();
    Thread.UncaughtExceptionHandler previous = Thread.getDefaultUncaughtExceptionHandler();
    Thread.setDefaultUncaughtExceptionHandler((thread, error) -> {
      if ("kafkafim-start-a".equals(thread.getName())) {
        escaped.set(error);
      } else if (previous != null) {
        previous.uncaughtException(thread, error);
      }
    });
    try (PipelineResources failing =
        new PipelineResources(
            new ListenerRelay(List.of(), List.of()),
            p ->
                (cluster, identity) -> {
                  throw new NoClassDefFoundError("some/Client");
                },
            PipelineSupervisorTest.FAST_SETTINGS)) {
      PipelineRuntime broken =
          new PipelineRuntime(
              "a",
              failing.acquire("a", TestConfigs.bind(yaml(A).replace("k1:9092", "127.0.0.1:9"))),
              r -> {});

      broken.start();

      await()
          .dontCatchUncaughtExceptions()
          .atMost(Duration.ofSeconds(30))
          .until(() -> broken.state() == PipelineState.FAILED_PREFLIGHT);
      assertThat(broken.failure()).contains("NoClassDefFoundError");
      await()
          .dontCatchUncaughtExceptions()
          .atMost(Duration.ofSeconds(10))
          .until(() -> escaped.get() != null);
      assertThat(escaped.get()).isInstanceOf(NoClassDefFoundError.class);
      assertThat(broken.retryIfFailed()).isTrue();
      broken.stop();
    } finally {
      Thread.setDefaultUncaughtExceptionHandler(previous);
    }
  }

  // ---- start and stop interleavings (#367), driven by a latch-controlled identity check ----

  /** An identity check that parks the starter in preflight until released or interrupted. */
  private static final class ParkedAuthCheck implements Preflight.AuthCheck {
    final CountDownLatch entered = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicInteger calls = new AtomicInteger();
    volatile Preflight.Status verdict = Preflight.Status.OK;

    @Override
    public Preflight.Status check(String cluster, String identity) {
      calls.incrementAndGet();
      entered.countDown();
      try {
        release.await(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        // stop() interrupts the checks: answer "unreachable" at once, as the real ones do.
        Thread.currentThread().interrupt();
        return Preflight.Status.unreachable("interrupted");
      }
      return verdict;
    }
  }

  private PipelineRuntime runtimeWith(PipelineResources pool) {
    return new PipelineRuntime(
        "a",
        pool.acquire("a", TestConfigs.bind(yaml(A).replace("k1:9092", "127.0.0.1:9"))),
        r -> {});
  }

  private static PipelineResources resourcesWith(Preflight.AuthCheck check) {
    return new PipelineResources(
        new ListenerRelay(List.of(), List.of()), p -> check, PipelineSupervisorTest.FAST_SETTINGS);
  }

  private static void awaitEntered(ParkedAuthCheck check) throws InterruptedException {
    assertThat(check.entered.await(30, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  void stopDuringPreflightEndsStoppedWithNothingOpen() throws Exception {
    ParkedAuthCheck check = new ParkedAuthCheck();
    try (PipelineResources pool = resourcesWith(check)) {
      PipelineRuntime parked = runtimeWith(pool);
      parked.start();
      awaitEntered(check);
      assertThat(parked.state()).isEqualTo(PipelineState.STARTING);

      parked.stop();

      assertThat(parked.state()).isEqualTo(PipelineState.STOPPED);
      assertThat(parked.source()).isNull();
      assertThat(parked.failure()).isNull();
      // A later start works: the cancelled start left no starter behind.
      assertThat(parked.retryIfFailed()).isFalse();
      parked.start();
      assertThat(parked.state()).isEqualTo(PipelineState.STARTING);
      parked.stop();
      assertThat(parked.state()).isEqualTo(PipelineState.STOPPED);
    }
  }

  @Test
  void verdictThatArrivesAfterTheStopMeansNothing() throws Exception {
    ParkedAuthCheck check = new ParkedAuthCheck();
    // A configuration verdict, which would fail the pipeline, if the stop had not cancelled it.
    check.verdict = Preflight.Status.rejected("bad key");
    try (PipelineResources pool = resourcesWith(check)) {
      PipelineRuntime parked = runtimeWith(pool);
      parked.start();
      awaitEntered(check);

      parked.stop();
      check.release.countDown();

      assertThat(parked.state()).isEqualTo(PipelineState.STOPPED);
      assertThat(parked.failure()).isNull();
    }
  }

  @Test
  void sourceFailedWhileStartingIsIgnoredBecauseThereIsNoSourceYet() throws Exception {
    ParkedAuthCheck check = new ParkedAuthCheck();
    try (PipelineResources pool = resourcesWith(check)) {
      PipelineRuntime parked = runtimeWith(pool);
      parked.start();
      awaitEntered(check);

      // A failure report of a source of an earlier run (or none): nothing to fail.
      parked.sourceFailed(null, "late report");

      assertThat(parked.state()).isEqualTo(PipelineState.STARTING);
      assertThat(parked.failure()).isNull();
      parked.stop();
      assertThat(parked.state()).isEqualTo(PipelineState.STOPPED);
    }
  }

  @Test
  void stopDuringTheRetryBackoffCancelsTheStartAtOnce() throws Exception {
    ParkedAuthCheck check = new ParkedAuthCheck();
    check.verdict = Preflight.Status.unreachable("es down");
    check.release.countDown();
    try (PipelineResources pool = resourcesWith(check)) {
      PipelineRuntime retrying = runtimeWith(pool);
      retrying.start();
      awaitEntered(check);
      assertThat(retrying.state()).isEqualTo(PipelineState.STARTING);

      retrying.stop();

      assertThat(retrying.state()).isEqualTo(PipelineState.STOPPED);
      assertThat(retrying.source()).isNull();
      assertThat(retrying.failure()).isNull();
    }
  }

  @Test
  void concurrentStopsDuringPreflightAllReturnStopped() throws Exception {
    ParkedAuthCheck check = new ParkedAuthCheck();
    try (PipelineResources pool = resourcesWith(check)) {
      PipelineRuntime parked = runtimeWith(pool);
      parked.start();
      awaitEntered(check);
      Thread other = Thread.ofPlatform().name("test-second-stop").start(parked::stop);

      parked.stop();
      other.join(Duration.ofSeconds(30));

      assertThat(other.isAlive()).isFalse();
      assertThat(parked.state()).isEqualTo(PipelineState.STOPPED);
    }
  }

  @Test
  void reconfigureNeedsContextThatHasThePipeline() {
    PipelineRuntime.Context other =
        resources.acquire("b", TestConfigs.bind(yaml(A.replace("a:", "b:"))));
    assertThatThrownBy(() -> runtime.reconfigure(other))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown pipeline 'a'");
    assertThat(runtime.context()).isSameAs(first);
  }
}
