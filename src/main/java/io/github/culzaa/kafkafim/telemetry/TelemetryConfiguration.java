package io.github.culzaa.kafkafim.telemetry;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.registry.otlp.OtlpMetricsSender;
import io.micrometer.tracing.Tracer;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * The telemetry beans (DESIGN §10): the §10 meters, the batch span recorder and the sampler that
 * keeps every failed batch. The supervisor feeds them; health indicators that need the supervisor
 * are declared by it.
 */
@Configuration(proxyBeanMethods = false)
@Profile("!validate")
public class TelemetryConfiguration {

  @Bean(destroyMethod = "close")
  KafkafimMeters kafkafimMeters(ObjectProvider<MeterRegistry> registry) {
    return new KafkafimMeters(registry.getIfAvailable(SimpleMeterRegistry::new));
  }

  /**
   * Wraps Spring Boot's OTLP metrics sender so non-finite points never leave ({@link
   * FiniteMetricsSender}, ADR-0055). A post-processor keeps Boot's sender, with its connection
   * details and TLS, rather than replacing it.
   */
  @Bean
  static BeanPostProcessor kafkafimFiniteMetricsSender() {
    return new BeanPostProcessor() {
      @Override
      public Object postProcessAfterInitialization(Object bean, String beanName) {
        return bean instanceof OtlpMetricsSender sender && !(bean instanceof FiniteMetricsSender)
            ? new FiniteMetricsSender(sender)
            : bean;
      }
    };
  }

  @Bean
  BatchTracer kafkafimBatchTracer(ObjectProvider<Tracer> tracer) {
    return new BatchTracer(tracer.getIfAvailable(() -> Tracer.NOOP));
  }

  /**
   * Replaces Spring Boot's {@code parentBased(traceIdRatioBased(probability))} with the same plus
   * "failed batches always" ({@link BatchSampler}).
   */
  @Bean
  Sampler kafkafimSampler(
      @Value("${management.tracing.sampling.probability:0.01}") double probability) {
    return BatchSampler.create(probability);
  }
}
