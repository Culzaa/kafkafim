package io.github.matabar.kafkafim.telemetry;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class PipelineMdcTest {

  @Test
  void holdsThePipelineNameUntilClosed() {
    try (var ignored = PipelineMdc.open("orders")) {
      assertThat(MDC.get(PipelineMdc.KEY)).isEqualTo("orders");
    }

    assertThat(MDC.get(PipelineMdc.KEY)).isNull();
  }

  @Test
  void theKeyIsTheOneLogbackAllowlists() throws Exception {
    String config =
        new String(
            getClass().getResourceAsStream("/logback-spring.xml").readAllBytes(),
            java.nio.charset.StandardCharsets.UTF_8);

    assertThat(config)
        .contains("<captureMdcAttributes>" + PipelineMdc.KEY + "</captureMdcAttributes>");
  }
}
