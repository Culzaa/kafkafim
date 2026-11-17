package io.github.matabar.kafkafim.telemetry;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One reason a pipeline is paused: a {@link PauseReason} kind plus the Elasticsearch cluster and
 * identity it names, the {@code reason}, {@code es_cluster} and {@code identity} labels of {@code
 * kafkafim_pipeline_pause_reason} (DESIGN §10). Both names come from the configuration, so the
 * series set is bounded by it; a reason that names neither (a blocked DLQ) carries {@code none},
 * and so does a breaker's identity (a breaker belongs to a cluster, not an identity).
 *
 * @param reason the kind
 * @param esCluster the Elasticsearch cluster, or {@code none}
 * @param identity the Elasticsearch identity, or {@code none}
 */
public record PauseCause(PauseReason reason, String esCluster, String identity) {

  /** Requires every component. */
  public PauseCause {
    Objects.requireNonNull(reason, "reason");
    Objects.requireNonNull(esCluster, "esCluster");
    Objects.requireNonNull(identity, "identity");
  }

  /** The breaker of {@code esCluster} is not closed. */
  public static PauseCause breaker(String esCluster) {
    return new PauseCause(PauseReason.ES_BREAKER, esCluster, TelemetryNames.NONE);
  }

  /** Elasticsearch rejects {@code identity} ({@code null}: an unauthenticated cluster). */
  public static PauseCause auth(String esCluster, @Nullable String identity) {
    return new PauseCause(
        PauseReason.ES_AUTH, esCluster, identity == null ? TelemetryNames.NONE : identity);
  }

  /** The pipeline's DLQ writer is blocked. */
  public static PauseCause dlqBlocked() {
    return new PauseCause(PauseReason.DLQ_BLOCKED, TelemetryNames.NONE, TelemetryNames.NONE);
  }
}
