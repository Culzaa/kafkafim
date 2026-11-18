package io.github.matabar.kafkafim.telemetry;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;
import io.opentelemetry.instrumentation.kafkaclients.common.v0_11.internal.KafkaProcessRequest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

/**
 * The hand-off of a record's {@code process} span context (ADR-0013, ADR-0049), without a broker:
 * the extractor is driven the way the OpenTelemetry wrapper drives it, at the end of the span.
 */
class ProcessSpanContextsTest {

  private static final String TRACE = "4bf92f3577b34da6a3ce929d0e0e4736";
  private static final String SPAN = "00f067aa0ba902b7";

  private final AttributesExtractor<KafkaProcessRequest, Void> extractor =
      ProcessSpanContexts.extractor();

  private static ConsumerRecord<byte[], byte[]> record(long offset) {
    return new ConsumerRecord<>("t", 0, offset, null, new byte[0]);
  }

  private static Context spanContext(boolean valid) {
    SpanContext span =
        valid
            ? SpanContext.create(TRACE, SPAN, TraceFlags.getSampled(), TraceState.getDefault())
            : SpanContext.getInvalid();
    return Span.wrap(span).storeInContext(Context.root());
  }

  private void end(ConsumerRecord<byte[], byte[]> record, Context context) {
    AttributesBuilder attributes = Attributes.builder();
    extractor.onEnd(attributes, context, new KafkaProcessRequest(record, "g", "c"), null, null);
  }

  @Test
  void takeReturnsTheTraceparentOfTheRecordsSpan() {
    ConsumerRecord<byte[], byte[]> record = record(1);

    end(record, spanContext(true));

    assertThat(ProcessSpanContexts.take(record)).isEqualTo("00-" + TRACE + "-" + SPAN + "-01");
  }

  @Test
  void takeRemovesTheEntry() {
    ConsumerRecord<byte[], byte[]> record = record(2);
    end(record, spanContext(true));

    assertThat(ProcessSpanContexts.take(record)).isNotNull();
    assertThat(ProcessSpanContexts.take(record)).isNull();
  }

  @Test
  void untracedRecordsHaveNoTraceparent() {
    assertThat(ProcessSpanContexts.take(record(3))).isNull();
  }

  @Test
  void invalidSpanContextsAreNotRecorded() {
    ConsumerRecord<byte[], byte[]> record = record(4);

    end(record, spanContext(false));

    assertThat(ProcessSpanContexts.take(record)).isNull();
  }

  @Test
  void anotherInstanceOfTheSamePositionGetsNothing() {
    ConsumerRecord<byte[], byte[]> traced = record(5);
    ConsumerRecord<byte[], byte[]> samePositionOtherInstance = record(5);
    end(traced, spanContext(true));

    assertThat(ProcessSpanContexts.take(samePositionOtherInstance)).isNull();
    assertThat(ProcessSpanContexts.take(traced)).isNotNull();
  }

  @Test
  void theExtractorAddsNoAttributes() {
    AttributesBuilder attributes = Attributes.builder();

    extractor.onStart(attributes, Context.root(), new KafkaProcessRequest(record(6), "g", "c"));
    end(record(6), spanContext(true));

    assertThat(attributes.build().isEmpty()).isTrue();
  }
}
