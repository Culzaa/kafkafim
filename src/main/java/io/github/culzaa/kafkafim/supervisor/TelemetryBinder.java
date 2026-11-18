package io.github.culzaa.kafkafim.supervisor;

import io.github.culzaa.kafkafim.config.KafkafimProperties;
import io.github.culzaa.kafkafim.config.Pipeline;
import io.github.culzaa.kafkafim.source.kafka.KafkaPipelineSource;
import io.github.culzaa.kafkafim.telemetry.KafkafimMeters;
import io.github.culzaa.kafkafim.telemetry.PauseCause;
import io.github.culzaa.kafkafim.telemetry.PipelineSpec;
import io.github.culzaa.kafkafim.telemetry.SourceProbe;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Keeps the §10 meters in line with the supervisor (DESIGN §10, §5.6): at startup and after every
 * applied reload it hands {@link KafkafimMeters} the pipelines in effect, with a {@link
 * SourceProbe} per pipeline that reads whatever source the pipeline currently runs. It also answers
 * the {@code kafka} readiness contributor.
 */
final class TelemetryBinder {

  private final PipelineSupervisor supervisor;
  private final KafkafimMeters meters;

  TelemetryBinder(PipelineSupervisor supervisor, KafkafimMeters meters) {
    this.supervisor = supervisor;
    this.meters = meters;
    supervisor.onTopologyChange(this::sync);
    sync();
  }

  /** Hands the configuration in effect to the meters. */
  void sync() {
    KafkafimProperties properties = supervisor.properties();
    Map<String, PipelineSpec> specs = new LinkedHashMap<>();
    Set<String> esClusters = new LinkedHashSet<>();
    properties
        .pipelines()
        .forEach((name, pipeline) -> {
          List<String> routes = new ArrayList<>();
          Set<String> clusters = new LinkedHashSet<>();
          Set<PipelineSpec.EsStream> streams = new LinkedHashSet<>();
          List<Pipeline.Route> configured = pipeline.routes();
          for (int i = 0; i < configured.size(); i++) {
            Pipeline.Route route = configured.get(i);
            routes.add(route.label(i));
            if (route.cluster() != null) {
              clusters.add(route.cluster()); // a drop route writes to none
              streams.add(new PipelineSpec.EsStream(route.cluster(), route.identity()));
            }
          }
          esClusters.addAll(clusters);
          specs.put(
              name,
              new PipelineSpec(
                  pipeline.source().cluster(),
                  pipeline.source().topics(),
                  routes,
                  clusters,
                  streams,
                  new Probe(name, clusters, streams)));
        });
    meters.sync(specs, esClusters, supervisor::breakerState);
  }

  /** For every started pipeline, whether its consumers are connected. */
  Map<String, Boolean> kafkaConnections() {
    Map<String, Boolean> connections = new LinkedHashMap<>();
    supervisor
        .states()
        .forEach((name, state) -> {
          if (state.isStarted()) {
            KafkaPipelineSource source = supervisor.sourceOrNull(name);
            connections.put(name, source != null && source.kafkaConnected());
          }
        });
    return connections;
  }

  /**
   * The causes a pipeline in {@code state} is paused for, as the labels of {@code
   * kafkafim_pipeline_pause_reason}: a supervisor reason string is matched against the pipeline's
   * configured clusters and identities, so the label values are always config names. Empty unless
   * the state is {@code paused}: the supervisor also holds reasons for a pipeline that is not
   * running (they apply when its source starts), and those pause nothing yet.
   */
  static Set<PauseCause> causes(
      PipelineState state,
      Set<String> reasons,
      Set<String> clusters,
      Set<PipelineSpec.EsStream> streams) {
    Set<PauseCause> causes = new LinkedHashSet<>();
    if (state != PipelineState.PAUSED) {
      return causes;
    }
    for (String cluster : clusters) {
      if (reasons.contains(PipelineRuntime.BREAKER_REASON_PREFIX + cluster)) {
        causes.add(PauseCause.breaker(cluster));
      }
    }
    for (PipelineSpec.EsStream stream : streams) {
      if (reasons.contains(PipelineRuntime.authReason(stream.esCluster(), stream.identity()))) {
        causes.add(PauseCause.auth(stream.esCluster(), stream.identity()));
      }
    }
    if (reasons.contains(PipelineRuntime.DLQ_BLOCKED_REASON)) {
      causes.add(PauseCause.dlqBlocked());
    }
    return causes;
  }

  /** The current source of one pipeline, read on every call. */
  private final class Probe implements SourceProbe {

    private final String pipeline;
    private final Set<String> clusters;
    private final Set<PipelineSpec.EsStream> streams;

    Probe(String pipeline, Set<String> clusters, Set<PipelineSpec.EsStream> streams) {
      this.pipeline = pipeline;
      this.clusters = clusters;
      this.streams = streams;
    }

    @Override
    public long inFlightBytes() {
      KafkaPipelineSource source = supervisor.sourceOrNull(pipeline);
      return source == null ? 0 : source.inFlightBytes();
    }

    @Override
    public int assignedPartitions() {
      KafkaPipelineSource source = supervisor.sourceOrNull(pipeline);
      return source == null ? 0 : source.assignedPartitions().size();
    }

    @Override
    public boolean connected() {
      KafkaPipelineSource source = supervisor.sourceOrNull(pipeline);
      return source != null && source.kafkaConnected();
    }

    @Override
    public boolean started() {
      PipelineState state = supervisor.stateOrNull(pipeline);
      return state != null && state.isStarted();
    }

    @Override
    public Set<PauseCause> pauseReasons() {
      PipelineState state = supervisor.stateOrNull(pipeline);
      if (state != PipelineState.PAUSED) {
        return Set.of();
      }
      try {
        return causes(state, supervisor.pauseReasons(pipeline), clusters, streams);
      } catch (IllegalArgumentException e) {
        return Set.of(); // a reload removed the pipeline between the two reads
      }
    }
  }
}
