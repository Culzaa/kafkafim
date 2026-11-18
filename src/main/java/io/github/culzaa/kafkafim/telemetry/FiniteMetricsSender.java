package io.github.culzaa.kafkafim.telemetry;

import com.google.protobuf.InvalidProtocolBufferException;
import io.micrometer.registry.otlp.OtlpMetricsSender;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drops the {@code NaN} and infinite data points from an OTLP metrics export before it is sent
 * (ADR-0055). Micrometer gauges report {@code NaN} for "no value" (a Kafka client's {@code *-avg}
 * and {@code *-max} metrics before their first sample, for one), and the OTLP registry exports them
 * as they are. The APM Server then writes the token {@code NaN} into a JSON document that
 * Elasticsearch rejects, and the points are lost. Skipping a point says "no value", which is what
 * {@code NaN} meant; a substituted zero would claim a measurement that never happened.
 *
 * <p>The body is parsed on every export and encoded again only when there is something to drop. A
 * body that does not parse is sent unchanged: a filter never costs the export.
 */
public final class FiniteMetricsSender implements OtlpMetricsSender {

  private static final Logger LOG = LoggerFactory.getLogger(FiniteMetricsSender.class);

  private final OtlpMetricsSender delegate;

  /**
   * Creates the decorator.
   *
   * @param delegate the sender that does the export
   */
  public FiniteMetricsSender(OtlpMetricsSender delegate) {
    this.delegate = delegate;
  }

  @Override
  public void send(Request request) throws Exception {
    byte[] filtered = filter(request.getMetricsData());
    if (filtered == request.getMetricsData()) {
      delegate.send(request);
      return;
    }
    delegate.send(
        Request.builder(filtered)
            .address(request.getAddress())
            .headers(request.getHeaders())
            .compressionMode(request.getCompressionMode())
            .build());
  }

  /** The body without non-finite points: the same array when there were none. */
  static byte[] filter(byte[] body) {
    ExportMetricsServiceRequest parsed;
    try {
      parsed = ExportMetricsServiceRequest.parseFrom(body);
    } catch (InvalidProtocolBufferException e) {
      LOG.debug("OTLP metrics body does not parse; sending it unchanged", e);
      return body;
    }
    ExportMetricsServiceRequest.Builder out = parsed.toBuilder();
    boolean[] dropped = {false};
    for (ResourceMetrics.Builder resource : out.getResourceMetricsBuilderList()) {
      for (ScopeMetrics.Builder scope : resource.getScopeMetricsBuilderList()) {
        List<Metric> kept =
            scope.getMetricsList().stream()
                .map(m -> finite(m, dropped))
                .filter(m -> m != null)
                .toList();
        if (kept.size() != scope.getMetricsCount()) {
          dropped[0] = true;
        }
        scope.clearMetrics().addAllMetrics(kept);
      }
    }
    return dropped[0] ? out.build().toByteArray() : body;
  }

  /** The metric without its non-finite points, or null when none is left. */
  private static Metric finite(Metric metric, boolean[] dropped) {
    Metric.Builder b = metric.toBuilder();
    switch (metric.getDataCase()) {
      case GAUGE -> {
        List<NumberDataPoint> points = finiteNumbers(metric.getGauge().getDataPointsList());
        dropped[0] |= points.size() != metric.getGauge().getDataPointsCount();
        if (points.isEmpty()) {
          return null;
        }
        b.getGaugeBuilder().clearDataPoints().addAllDataPoints(points);
      }
      case SUM -> {
        List<NumberDataPoint> points = finiteNumbers(metric.getSum().getDataPointsList());
        dropped[0] |= points.size() != metric.getSum().getDataPointsCount();
        if (points.isEmpty()) {
          return null;
        }
        b.getSumBuilder().clearDataPoints().addAllDataPoints(points);
      }
      case HISTOGRAM -> {
        for (HistogramDataPoint.Builder p : b.getHistogramBuilder().getDataPointsBuilderList()) {
          if (p.hasSum() && !Double.isFinite(p.getSum())) {
            p.clearSum();
            dropped[0] = true;
          }
          if (p.hasMax() && !Double.isFinite(p.getMax())) {
            p.clearMax();
            dropped[0] = true;
          }
          if (p.hasMin() && !Double.isFinite(p.getMin())) {
            p.clearMin();
            dropped[0] = true;
          }
        }
      }
      default -> {}
    }
    return b.build();
  }

  private static List<NumberDataPoint> finiteNumbers(List<NumberDataPoint> points) {
    return points.stream()
        .filter(p ->
            p.getValueCase() != NumberDataPoint.ValueCase.AS_DOUBLE
                || Double.isFinite(p.getAsDouble()))
        .toList();
  }
}
