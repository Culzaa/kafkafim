package io.github.culzaa.kafkafim.telemetry;

import org.slf4j.MDC;

/**
 * The {@code pipeline} MDC entry (ADR-0048): the one key {@code logback-spring.xml} allowlists, so
 * that a log record written for a pipeline carries the pipeline name as an attribute over OTLP.
 * Threads that work for one pipeline (its consumer threads, its bulk senders, its DLQ writer tasks)
 * hold it while they run a task, and release it afterwards, so a pooled or reused thread never
 * carries a stale name. Threads owned by a client library (the Kafka producer's I/O thread, the
 * Elasticsearch HTTP client's) are not covered.
 */
public final class PipelineMdc {

  /** The MDC key, the name in {@code captureMdcAttributes}. */
  public static final String KEY = "pipeline";

  private PipelineMdc() {}

  /**
   * Sets {@value #KEY} on the current thread until the result is closed.
   *
   * @param pipeline the pipeline name
   * @return closes by removing the entry
   */
  public static MDC.MDCCloseable open(String pipeline) {
    return MDC.putCloseable(KEY, pipeline);
  }
}
