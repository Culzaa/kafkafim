package io.github.matabar.kafkafim.telemetry;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.util.LogbackMDCAdapter;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.instrumentation.logback.appender.v1_0.OpenTelemetryAppender;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.data.LogRecordData;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;
import io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Only the MDC keys named in {@code captureMdcAttributes} in {@code logback-spring.xml} reach the
 * OTLP log record (DESIGN §10 Logs): the real file is loaded into a fresh logger context, with an
 * in-memory exporter behind the OpenTelemetry appender.
 */
class MdcAllowlistTest {

  private final List<LogRecordData> exported = new CopyOnWriteArrayList<>();
  private LoggerContext context;
  private OpenTelemetrySdk sdk;

  @BeforeEach
  void loadLogbackConfig() throws Exception {
    sdk =
        OpenTelemetrySdk.builder()
            .setLoggerProvider(
                SdkLoggerProvider.builder()
                    .addLogRecordProcessor(SimpleLogRecordProcessor.create(new Capture()))
                    .build())
            .build();
    context = new LoggerContext();
    context.setMDCAdapter(new LogbackMDCAdapter());
    JoranConfigurator configurator = new JoranConfigurator();
    configurator.setContext(context);
    configurator.doConfigure(getClass().getResource("/logback-spring.xml"));
    ((OpenTelemetryAppender) context.getLogger("ROOT").getAppender("OTLP")).setOpenTelemetry(sdk);
  }

  @AfterEach
  void tearDown() {
    context.stop();
    sdk.close();
  }

  @Test
  void exportsAllowlistedMdcKeyAndDropsTheOthers() {
    context.getMDCAdapter().put("pipeline", "orders");
    context.getMDCAdapter().put("document", "{\"card\":\"4111\"}");
    context.getLogger(MdcAllowlistTest.class).info("hello");

    assertThat(exported).hasSize(1);
    var attributes = exported.get(0).getAttributes();
    assertThat(attributes.get(AttributeKey.stringKey("pipeline"))).isEqualTo("orders");
    assertThat(attributes.asMap().keySet().stream().map(AttributeKey::getKey))
        .noneMatch(k -> k.contains("document"));
  }

  private final class Capture implements LogRecordExporter {
    @Override
    public CompletableResultCode export(Collection<LogRecordData> logs) {
      exported.addAll(logs);
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode flush() {
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown() {
      return CompletableResultCode.ofSuccess();
    }
  }
}
