package io.github.culzaa.kafkafim.source.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import org.junit.jupiter.api.Test;

class ConsumerMetricsListenerTest {

  private static final Set<String> PIPELINES = Set.of("orders", "orders-1", "billing");

  @Test
  void collidingNamesResolveByTheThreadSuffix() {
    assertThat(ConsumerMetricsListener.pipelineOf("g.kafkafim-orders-1", PIPELINES))
        .isEqualTo("orders");
    assertThat(ConsumerMetricsListener.pipelineOf("g.kafkafim-orders-1-0", PIPELINES))
        .isEqualTo("orders-1");
    assertThat(ConsumerMetricsListener.pipelineOf("g.kafkafim-orders-0", PIPELINES))
        .isEqualTo("orders");
  }

  @Test
  void placeholderGroupPrefixAndBareIdsAreHandled() {
    assertThat(
            ConsumerMetricsListener.pipelineOf(
                "not.managed.by.Spring.kafkafim-billing-3", PIPELINES))
        .isEqualTo("billing");
    assertThat(ConsumerMetricsListener.pipelineOf("kafkafim-billing-0", PIPELINES))
        .isEqualTo("billing");
  }

  @Test
  void metadataAndForeignConsumersGetNoSeries() {
    assertThat(ConsumerMetricsListener.pipelineOf("x.kafkafim-orders-metadata", PIPELINES))
        .isNull();
    assertThat(ConsumerMetricsListener.pipelineOf("x.kafkafim-other-0", PIPELINES)).isNull();
    assertThat(ConsumerMetricsListener.pipelineOf("xkafkafim-billing-0", PIPELINES)).isNull();
    assertThat(ConsumerMetricsListener.pipelineOf(null, PIPELINES)).isNull();
  }
}
