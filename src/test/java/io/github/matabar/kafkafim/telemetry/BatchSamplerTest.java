package io.github.matabar.kafkafim.telemetry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingDecision;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/** {@link BatchSampler} and the sampler bean's configuration (DESIGN §10, ADR-0029). */
class BatchSamplerTest {

  @ParameterizedTest
  @ValueSource(doubles = {-0.1, 1.1, Double.NaN, Double.POSITIVE_INFINITY})
  void rejectsProbabilitiesOutsideZeroToOne(double probability) {
    assertThatThrownBy(() -> BatchSampler.create(probability))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("management.tracing.sampling.probability");
  }

  @ParameterizedTest
  @ValueSource(doubles = {0, 0.01, 1})
  void acceptsTheBounds(double probability) {
    assertThat(BatchSampler.create(probability)).isNotNull();
  }

  @Test
  void theBeanDefaultsToOnePercentWhenThePropertyIsAbsent() {
    new ApplicationContextRunner()
        .withUserConfiguration(TelemetryConfiguration.class)
        .run(context ->
            assertThat(context.getBean(Sampler.class).getDescription()).contains("ratio=0.01"));
  }

  @Test
  void theBeanFollowsTheProperty() {
    new ApplicationContextRunner()
        .withUserConfiguration(TelemetryConfiguration.class)
        .withPropertyValues("management.tracing.sampling.probability=0.5")
        .run(context ->
            assertThat(context.getBean(Sampler.class).getDescription()).contains("ratio=0.5"));
  }

  @Test
  void theBeanFailsToStartOnAnInvalidProbability() {
    new ApplicationContextRunner()
        .withUserConfiguration(TelemetryConfiguration.class)
        .withPropertyValues("management.tracing.sampling.probability=1.5")
        .run(context ->
            assertThat(context)
                .hasFailed()
                .getFailure()
                .hasStackTraceContaining("management.tracing.sampling.probability"));
  }

  private static final String TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";
  private static final Attributes FAILED =
      Attributes.of(AttributeKey.stringKey(BatchTracer.ERROR_TYPE), "java.net.ConnectException");

  private static Context parent(boolean sampled) {
    SpanContext parent =
        SpanContext.create(
            TRACE_ID,
            "00f067aa0ba902b7",
            sampled ? TraceFlags.getSampled() : TraceFlags.getDefault(),
            TraceState.getDefault());
    return Span.wrap(parent).storeInContext(Context.root());
  }

  private static SamplingDecision decide(
      double probability, Context parent, String name, Attributes attributes) {
    return BatchSampler.create(probability)
        .shouldSample(parent, TRACE_ID, name, SpanKind.INTERNAL, attributes, List.of())
        .getDecision();
  }

  @Test
  void describesItselfWithTheRatio() {
    assertThat(BatchSampler.create(0.25).getDescription())
        .isEqualTo("KafkafimBatchSampler{errors=always,parentBased,ratio=0.25}");
  }

  @Test
  void rootSpansFollowTheRatioAtTheBounds() {
    assertThat(decide(0, Context.root(), "pipeline.batch", Attributes.empty()))
        .isEqualTo(SamplingDecision.DROP);
    assertThat(decide(1, Context.root(), "pipeline.batch", Attributes.empty()))
        .isEqualTo(SamplingDecision.RECORD_AND_SAMPLE);
  }

  @Test
  void failedBatchesAreSampledWhateverTheRatioOrTheirParentDecided() {
    assertThat(decide(0, Context.root(), BatchTracer.BATCH_SPAN, FAILED))
        .isEqualTo(SamplingDecision.RECORD_AND_SAMPLE);
    assertThat(decide(0, parent(false), BatchTracer.BATCH_SPAN, FAILED))
        .isEqualTo(SamplingDecision.RECORD_AND_SAMPLE);
  }

  @Test
  void healthyBatchesFollowTheirParent() {
    assertThat(decide(0, parent(true), BatchTracer.BATCH_SPAN, Attributes.empty()))
        .isEqualTo(SamplingDecision.RECORD_AND_SAMPLE);
    assertThat(decide(1, parent(false), BatchTracer.BATCH_SPAN, Attributes.empty()))
        .isEqualTo(SamplingDecision.DROP);
  }

  @Test
  void spansOtherThanBatchesFollowTheirParentEvenWithAnError() {
    assertThat(decide(1, parent(false), "es.bulk", FAILED)).isEqualTo(SamplingDecision.DROP);
    assertThat(decide(0, parent(true), "es.bulk", FAILED))
        .isEqualTo(SamplingDecision.RECORD_AND_SAMPLE);
  }
}
