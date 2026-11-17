package io.github.matabar.kafkafim.telemetry;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import java.util.Locale;
import org.jspecify.annotations.Nullable;

/**
 * A parsed W3C Trace Context {@code traceparent} header ({@code 00-<32 hex trace id>-<16 hex span
 * id>-<2 hex flags>}), as carried by Kafka records.
 *
 * @param traceId 32 lowercase hex characters, not all zero
 * @param spanId 16 lowercase hex characters, not all zero
 * @param sampled the {@code sampled} flag
 */
public record TraceParent(String traceId, String spanId, boolean sampled) {

  /** The Kafka header that carries it. */
  public static final String HEADER = "traceparent";

  /**
   * Parses a header value; anything that is not a valid version-00 value (or a later version with
   * the same prefix, as the specification allows) is {@code null}.
   */
  public static @Nullable TraceParent parse(@Nullable String value) {
    if (value == null) {
      return null;
    }
    String text = value.trim().toLowerCase(Locale.ROOT);
    if (text.length() < 55
        || text.charAt(2) != '-'
        || text.charAt(35) != '-'
        || text.charAt(52) != '-') {
      return null;
    }
    String version = text.substring(0, 2);
    if (!hex(version) || version.equals("ff") || (version.equals("00") && text.length() != 55)) {
      return null;
    }
    if (text.length() > 55 && text.charAt(55) != '-') {
      return null;
    }
    String traceId = text.substring(3, 35);
    String spanId = text.substring(36, 52);
    String flags = text.substring(53, 55);
    if (!hex(traceId) || !hex(spanId) || !hex(flags) || zero(traceId) || zero(spanId)) {
      return null;
    }
    return new TraceParent(traceId, spanId, (Integer.parseInt(flags, 16) & 1) == 1);
  }

  /** The version-00 header value of a span context. */
  public static String format(SpanContext context) {
    return "00-"
        + context.getTraceId()
        + "-"
        + context.getSpanId()
        + "-"
        + (context.isSampled() ? "01" : "00");
  }

  /**
   * An OpenTelemetry context whose current span is this remote span, for a span started under it.
   */
  public Context toContext() {
    SpanContext remote =
        SpanContext.createFromRemoteParent(
            traceId,
            spanId,
            sampled ? TraceFlags.getSampled() : TraceFlags.getDefault(),
            TraceState.getDefault());
    return Context.root().with(Span.wrap(remote));
  }

  private static boolean hex(String text) {
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (!(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f')) {
        return false;
      }
    }
    return true;
  }

  private static boolean zero(String text) {
    for (int i = 0; i < text.length(); i++) {
      if (text.charAt(i) != '0') {
        return false;
      }
    }
    return true;
  }
}
