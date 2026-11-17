package io.github.matabar.kafkafim.telemetry;

import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;
import io.opentelemetry.instrumentation.kafkaclients.common.v0_11.internal.KafkaProcessRequest;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jspecify.annotations.Nullable;

/**
 * The context of the Kafka {@code process} span of each polled record, kept for the hand-off
 * (ADR-0013, DESIGN §10).
 *
 * <p>The OpenTelemetry {@code kafka-clients} wrapper starts the {@code process} span of a record
 * when Spring Kafka iterates the polled records and ends it at the next step, so no span is current
 * once the batch listener runs, and kafkafim processes the records on other threads anyway. The
 * span's context is therefore recorded at the end of the span, by an attribute extractor the {@link
 * io.opentelemetry.instrumentation.kafkaclients.v2_6.KafkaTelemetry} is built with ({@link
 * #extractor()}), and read back by identity of the record ({@link #take}) when the source wraps it
 * in a {@code SourceRecord}, on the same consumer thread. Both then use the same {@code
 * ConsumerRecord} instance. An entry whose record is never taken (a record that is not handed on)
 * goes away with the record, because the keys are weak.
 *
 * <p>The extractor type names {@code KafkaProcessRequest} from the library's internal package, the
 * only way the library offers to see a process span; the artifact is alpha and pinned to the
 * OpenTelemetry BOM, and the integration test fails if the hook stops working.
 */
public final class ProcessSpanContexts {

  private static final Map<ConsumerRecord<?, ?>, String> CONTEXTS =
      Collections.synchronizedMap(new WeakHashMap<>());

  private static final AttributesExtractor<KafkaProcessRequest, Void> EXTRACTOR =
      new AttributesExtractor<>() {
        @Override
        public void onStart(
            AttributesBuilder attributes, Context parentContext, KafkaProcessRequest request) {}

        @Override
        public void onEnd(
            AttributesBuilder attributes,
            Context context,
            KafkaProcessRequest request,
            @Nullable Void response,
            @Nullable Throwable error) {
          SpanContext span = Span.fromContext(context).getSpanContext();
          if (span.isValid()) {
            CONTEXTS.put(request.getRecord(), TraceParent.format(span));
          }
        }
      };

  private ProcessSpanContexts() {}

  /** The extractor to add with {@code addConsumerProcessAttributesExtractors}. */
  public static AttributesExtractor<KafkaProcessRequest, Void> extractor() {
    return EXTRACTOR;
  }

  /**
   * Removes and returns the {@code traceparent} of the record's process span.
   *
   * @return the header value, or {@code null} when the record was not traced (no wrapper, or the
   *     span was not recorded)
   */
  public static @Nullable String take(ConsumerRecord<?, ?> record) {
    return CONTEXTS.remove(record);
  }
}
