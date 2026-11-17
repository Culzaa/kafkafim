package io.github.matabar.kafkafim.telemetry;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.kafkaclients.v2_6.KafkaTelemetry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * The one {@link KafkaTelemetry} of the process (ADR-0013): the OpenTelemetry {@code kafka-clients}
 * library is the only tracing mechanism for the Kafka consumers and the DLQ producer. The
 * OpenTelemetry Java agent is never attached and Spring Kafka's observation is never enabled;
 * either would add a second span for the same client.
 *
 * <p>Built from the {@link OpenTelemetry} that Spring Boot auto-configures. Not created under the
 * {@code validate} profile, like the other telemetry beans (DESIGN §5.6): that run has no consumers
 * or producer to instrument and exports nothing.
 */
@Configuration(proxyBeanMethods = false)
@Profile("!validate")
public class KafkaTelemetryConfiguration {

  @Bean
  KafkaTelemetry kafkafimKafkaTelemetry(OpenTelemetry openTelemetry) {
    return create(openTelemetry);
  }

  /**
   * The telemetry as the bean builds it, also for tests: the consumers' process spans are recorded
   * in {@link ProcessSpanContexts}, so the batch and DLQ spans can be their children.
   */
  public static KafkaTelemetry create(OpenTelemetry openTelemetry) {
    return KafkaTelemetry.builder(openTelemetry)
        .addConsumerProcessAttributesExtractors(ProcessSpanContexts.extractor())
        .build();
  }

  /**
   * The one binder of the Kafka clients' metrics: Micrometer's, on the registry of the {@code
   * kafkafim_*} series. Spring Boot's {@link OpenTelemetry} has no exporting meter provider, so the
   * library's own metrics reporter is not used.
   */
  @Bean
  KafkaClientMeters kafkafimKafkaClientMeters(ObjectProvider<MeterRegistry> registry) {
    return new KafkaClientMeters(registry.getIfAvailable(SimpleMeterRegistry::new));
  }
}
