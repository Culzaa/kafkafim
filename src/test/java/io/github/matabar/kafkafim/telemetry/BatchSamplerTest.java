package io.github.matabar.kafkafim.telemetry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.opentelemetry.sdk.trace.samplers.Sampler;
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
}
