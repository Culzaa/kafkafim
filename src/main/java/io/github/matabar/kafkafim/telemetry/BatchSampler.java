package io.github.matabar.kafkafim.telemetry;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;
import java.util.List;

/**
 * Head sampling of batches with errors always kept (DESIGN §10, ADR-0029). A {@code pipeline.batch}
 * span that carries {@code error.type} is always sampled, whatever its parent decided: a failed
 * batch is never lost. Every other span is sampled <em>parent-based</em>: a span with a parent
 * follows the parent's sampled flag, so a batch parented on a Kafka {@code process} span is kept
 * exactly when that span is, and its {@code es.bulk} children follow the batch; a root span (a
 * batch without a Kafka parent, HTTP server spans) is sampled by trace id at {@code
 * management.tracing.sampling.probability}. A failed batch whose parent was not sampled is
 * therefore exported without its Kafka process parent. Replaces Spring Boot's default sampler,
 * which is the same without the error rule.
 */
public final class BatchSampler implements Sampler {

  private static final AttributeKey<String> ERROR_TYPE =
      AttributeKey.stringKey(BatchTracer.ERROR_TYPE);

  private final Sampler parentBased;
  private final double probability;

  private BatchSampler(double probability) {
    this.probability = probability;
    this.parentBased = Sampler.parentBased(Sampler.traceIdRatioBased(probability));
  }

  /**
   * The sampler to install.
   *
   * @param probability fraction of root spans without errors to keep, in {@code [0, 1]}
   * @throws IllegalArgumentException if it is NaN or outside {@code [0, 1]}
   */
  public static Sampler create(double probability) {
    if (Double.isNaN(probability) || probability < 0 || probability > 1) {
      throw new IllegalArgumentException(
          "management.tracing.sampling.probability must be between 0 and 1, got " + probability);
    }
    return new BatchSampler(probability);
  }

  @Override
  public SamplingResult shouldSample(
      Context parentContext,
      String traceId,
      String name,
      SpanKind spanKind,
      Attributes attributes,
      List<LinkData> parentLinks) {
    if (BatchTracer.BATCH_SPAN.equals(name) && attributes.get(ERROR_TYPE) != null) {
      return SamplingResult.recordAndSample();
    }
    return parentBased.shouldSample(
        parentContext, traceId, name, spanKind, attributes, parentLinks);
  }

  @Override
  public String getDescription() {
    return "KafkafimBatchSampler{errors=always,parentBased,ratio=" + probability + "}";
  }
}
