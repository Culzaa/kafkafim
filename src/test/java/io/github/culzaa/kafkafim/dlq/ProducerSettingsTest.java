package io.github.culzaa.kafkafim.dlq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.culzaa.kafkafim.config.KafkaCluster;
import io.github.culzaa.kafkafim.config.KafkaCluster.KafkaIdentity;
import io.github.culzaa.kafkafim.config.KafkaCluster.Keystore;
import io.github.culzaa.kafkafim.config.KafkaCluster.Sasl;
import io.github.culzaa.kafkafim.config.KafkaCluster.SecurityProtocol;
import io.github.culzaa.kafkafim.config.KafkaCluster.Trust;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ProducerSettingsTest {

  private static final Sasl SCRAM = new Sasl("SCRAM-SHA-256", "dlq-writer", "s3cr3t");

  private static KafkaCluster cluster(
      SecurityProtocol protocol, Trust trust, Map<String, String> properties) {
    return new KafkaCluster(
        List.of("k1:9093", "k2:9093"), protocol, trust, Map.of(), null, properties);
  }

  @Test
  void plaintextClusterGetsKafkafimSettings() {
    Map<String, Object> settings =
        ProducerSettings.forIdentity("prod-a", cluster(null, null, Map.of()), null, null);

    assertThat(settings)
        .containsEntry("bootstrap.servers", "k1:9093,k2:9093")
        .containsEntry("security.protocol", "PLAINTEXT")
        .containsEntry("client.id", "kafkafim-dlq.prod-a")
        .containsEntry("acks", "all")
        .containsEntry("enable.idempotence", true)
        .containsEntry("retries", Integer.MAX_VALUE)
        .containsEntry("max.in.flight.requests.per.connection", 5)
        .containsEntry("delivery.timeout.ms", 120_000)
        .containsEntry("key.serializer", ByteArraySerializer.class)
        .containsEntry("value.serializer", ByteArraySerializer.class)
        .containsEntry("compression.type", "lz4")
        .containsEntry("max.request.size", 16 * 1024 * 1024)
        .containsEntry("max.block.ms", 10_000)
        .doesNotContainKeys("transactional.id", "sasl.jaas.config", "ssl.truststore.location");
    ProducerConfig parsed = new ProducerConfig(settings);
    assertThat(parsed.getBoolean("enable.idempotence")).isTrue();
    assertThat(parsed.getString("acks")).isEqualTo("-1");
  }

  @Test
  void saslSslClusterWithScramIdentity() {
    Trust trust = new Trust("/etc/trust.p12", "trust-pass", "PKCS12");
    Map<String, Object> settings =
        ProducerSettings.forIdentity(
            "prod-a",
            cluster(SecurityProtocol.SASL_SSL, trust, Map.of()),
            "dlq-writer",
            new KafkaIdentity(SCRAM, null));

    assertThat(settings)
        .containsEntry("client.id", "kafkafim-dlq.prod-a.dlq-writer")
        .containsEntry("security.protocol", "SASL_SSL")
        .containsEntry("ssl.truststore.location", "/etc/trust.p12")
        .containsEntry("ssl.truststore.password", "trust-pass")
        .containsEntry("ssl.truststore.type", "PKCS12")
        .containsEntry("sasl.mechanism", "SCRAM-SHA-256")
        .containsEntry(
            "sasl.jaas.config",
            "org.apache.kafka.common.security.scram.ScramLoginModule required"
                + " username=\"dlq-writer\" password=\"s3cr3t\";");
    new ProducerConfig(settings);
  }

  @Test
  void sslClusterWithKeystoreIdentity() {
    Trust trust = new Trust("/etc/trust.p12", null, null);
    Keystore keystore = new Keystore("/etc/ks.p12", "ks-pass", "key-pass", null);
    Map<String, Object> settings =
        ProducerSettings.forIdentity(
            "prod-a",
            cluster(SecurityProtocol.SSL, trust, Map.of()),
            "platform-cert",
            new KafkaIdentity(null, keystore));

    assertThat(settings)
        .containsEntry("ssl.keystore.location", "/etc/ks.p12")
        .containsEntry("ssl.keystore.password", "ks-pass")
        .containsEntry("ssl.key.password", "key-pass")
        .doesNotContainKeys("ssl.truststore.password", "ssl.keystore.type", "sasl.mechanism");
  }

  @Test
  void passthroughOverridesDefaultsAndDropsConsumerOnlyKeys() {
    Map<String, Object> settings =
        ProducerSettings.forIdentity(
            "prod-a",
            cluster(
                null,
                null,
                Map.of(
                    "compression.type", "zstd",
                    "linger.ms", "20",
                    "request.timeout.ms", "15000",
                    "fetch.min.bytes", "1024",
                    "isolation.level", "read_committed",
                    "my.plugin.setting", "x")),
            null,
            null);

    assertThat(settings)
        .containsEntry("compression.type", "zstd")
        .containsEntry("linger.ms", "20")
        .containsEntry("request.timeout.ms", "15000")
        .containsEntry("my.plugin.setting", "x")
        .doesNotContainKeys("fetch.min.bytes", "isolation.level");
  }

  @Test
  void idempotenceDefaultsToTrueWithFiveInFlight() {
    Map<String, Object> settings =
        ProducerSettings.forIdentity("prod-a", cluster(null, null, Map.of()), null, null);

    assertThat(settings)
        .containsEntry("enable.idempotence", true)
        .containsEntry("max.in.flight.requests.per.connection", 5);
  }

  @Test
  void idempotenceTrueIsAccepted() {
    Map<String, Object> settings =
        ProducerSettings.forIdentity(
            "prod-a", cluster(null, null, Map.of("enable.idempotence", "true")), null, null);

    assertThat(settings)
        .containsEntry("enable.idempotence", true)
        .containsEntry("max.in.flight.requests.per.connection", 5);
  }

  @Test
  void idempotenceFalseIsAcceptedAndKeepsRetriesOrdered() {
    Map<String, Object> settings =
        ProducerSettings.forIdentity(
            "prod-a", cluster(null, null, Map.of("enable.idempotence", "false")), null, null);

    assertThat(settings)
        .containsEntry("enable.idempotence", false)
        .containsEntry("max.in.flight.requests.per.connection", 1)
        .containsEntry("acks", "all")
        .containsEntry("retries", Integer.MAX_VALUE);
    ProducerConfig parsed = new ProducerConfig(settings);
    assertThat(parsed.getBoolean("enable.idempotence")).isFalse();
    assertThat(parsed.getString("acks")).isEqualTo("-1");
  }

  @Test
  void withoutIdempotenceTheProducerHasNoTransactionManagerToTurnFatal() {
    Map<String, Object> settings =
        ProducerSettings.forIdentity(
            "prod-a", cluster(null, null, Map.of("enable.idempotence", "false")), null, null);
    settings.put("bootstrap.servers", "127.0.0.1:1");

    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(settings)) {
      // kafka-clients creates a TransactionManager only for an idempotent or transactional
      // producer; that class is the only source of the fatal error state DlqFailure guards.
      Object transactionManager = readField(producer, "transactionManager");
      assertThat(transactionManager).isNull();
    }
  }

  private static Object readField(Object target, String name) {
    try {
      java.lang.reflect.Field field = target.getClass().getDeclaredField(name);
      field.setAccessible(true);
      return field.get(target);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"yes", "TRUE", "1", ""})
  void invalidIdempotenceValueIsRejected(String value) {
    KafkaCluster cluster = cluster(null, null, Map.of("enable.idempotence", value));

    assertThatThrownBy(() -> ProducerSettings.forIdentity("prod-a", cluster, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("enable.idempotence")
        .hasMessageContaining("prod-a");
  }

  @Test
  void ownedKeysArePinnedToTheKafkaClientsConstants() {
    assertThat(ProducerSettings.OWNED_KEYS)
        .containsExactlyInAnyOrder(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
            CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
            ProducerConfig.CLIENT_ID_CONFIG,
            ProducerConfig.ACKS_CONFIG,
            ProducerConfig.RETRIES_CONFIG,
            ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            ProducerConfig.TRANSACTIONAL_ID_CONFIG,
            ProducerConfig.INTERCEPTOR_CLASSES_CONFIG);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "bootstrap.servers",
        "security.protocol",
        "client.id",
        "acks",
        "retries",
        "max.in.flight.requests.per.connection",
        "delivery.timeout.ms",
        "key.serializer",
        "value.serializer",
        "transactional.id",
        "interceptor.classes"
      })
  void passthroughMayNotSetOwnedKeys(String key) {
    KafkaCluster cluster = cluster(null, null, Map.of(key, "secret-value"));

    assertThatThrownBy(() -> ProducerSettings.forIdentity("prod-a", cluster, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(key)
        .hasMessageContaining("prod-a")
        .hasMessageNotContaining("secret-value");
  }

  @Test
  void derivedSecurityKeysAreOwnedOnlyWhenTheirBlockIsSet() {
    Map<String, String> jaas = Map.of("sasl.jaas.config", "custom");
    KafkaCluster sasl = cluster(SecurityProtocol.SASL_SSL, null, jaas);

    assertThatThrownBy(() ->
            ProducerSettings.forIdentity(
                "prod-a", sasl, "dlq-writer", new KafkaIdentity(SCRAM, null)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("sasl.jaas.config");

    Map<String, Object> oauth =
        ProducerSettings.forIdentity(
            "prod-a", sasl, "oauth", new KafkaIdentity(new Sasl("OAUTHBEARER", null, null), null));
    assertThat(oauth)
        .containsEntry("sasl.jaas.config", "custom")
        .containsEntry("sasl.mechanism", "OAUTHBEARER");

    KafkaCluster truststoreInPassthrough =
        cluster(SecurityProtocol.SSL, null, Map.of("ssl.truststore.location", "/x"));
    assertThat(ProducerSettings.forIdentity("prod-a", truststoreInPassthrough, null, null))
        .containsEntry("ssl.truststore.location", "/x");
    KafkaCluster both =
        cluster(
            SecurityProtocol.SSL,
            new Trust("/etc/trust.p12", null, null),
            Map.of("ssl.truststore.location", "/x"));
    assertThatThrownBy(() -> ProducerSettings.forIdentity("prod-a", both, null, null))
        .hasMessageContaining("ssl.truststore.location");
  }

  @Test
  void jaasValuesAreEscaped() {
    Map<String, Object> settings =
        ProducerSettings.forIdentity(
            "prod-a",
            cluster(SecurityProtocol.SASL_PLAINTEXT, null, Map.of()),
            "w",
            new KafkaIdentity(new Sasl("PLAIN", "u\"x", "p\\\"y"), null));

    assertThat(settings.get("sasl.jaas.config"))
        .isEqualTo(
            "org.apache.kafka.common.security.plain.PlainLoginModule required"
                + " username=\"u\\\"x\" password=\"p\\\\\\\"y\";");
  }

  @Test
  void redactedHidesSecrets() {
    Map<String, Object> settings =
        ProducerSettings.forIdentity(
            "prod-a",
            cluster(SecurityProtocol.SASL_SSL, new Trust("/t", "trust-pass", null), Map.of()),
            "dlq-writer",
            new KafkaIdentity(SCRAM, new Keystore("/k", "ks-pass", "key-pass", null)));

    Map<String, Object> redacted = ProducerSettings.redacted(settings);

    assertThat(redacted.toString())
        .doesNotContain("s3cr3t", "trust-pass", "ks-pass", "key-pass")
        .contains("sasl.jaas.config=[hidden]", "client.id=kafkafim-dlq.prod-a.dlq-writer");
  }

  @Test
  void adminSettingsKeepConnectionAndSecurityKeysOnly() {
    Map<String, Object> producer =
        ProducerSettings.forIdentity(
            "prod-a",
            cluster(SecurityProtocol.SASL_SSL, new Trust("/t", "trust-pass", null), Map.of()),
            "dlq-writer",
            new KafkaIdentity(SCRAM, null));

    Map<String, Object> admin = ProducerSettings.adminSettings(producer);

    assertThat(admin)
        .containsEntry("bootstrap.servers", "k1:9093,k2:9093")
        .containsEntry("security.protocol", "SASL_SSL")
        .containsEntry("client.id", "kafkafim-dlq-admin.prod-a.dlq-writer")
        .containsKeys("sasl.jaas.config", "sasl.mechanism", "ssl.truststore.location")
        .doesNotContainKeys(
            "acks", "key.serializer", "enable.idempotence", "compression.type", "max.block.ms");
    new AdminClientConfig(admin);
  }

  @Test
  void clientIdsOfNamesContainingDashesDoNotCollide() {
    assertThat(ProducerSettings.clientId("a-b", "c"))
        .isNotEqualTo(ProducerSettings.clientId("a", "b-c"));
    assertThat(ProducerSettings.clientId("a", "b"))
        .isNotEqualTo(ProducerSettings.clientId("a-b", null));
    assertThat(ProducerSettings.clientId("a", null)).isEqualTo("kafkafim-dlq.a");
  }

  @Test
  void adminClientIdNeverEqualsAnyProducerClientId() {
    String admin =
        (String)
            ProducerSettings.adminSettings(Map.of("client.id", ProducerSettings.clientId("a", "b")))
                .get("client.id");

    assertThat(admin)
        .isNotEqualTo(ProducerSettings.clientId("a", "b-admin"))
        .isNotEqualTo(ProducerSettings.clientId("a", "b.admin"))
        .isNotEqualTo(ProducerSettings.clientId("a", "admin"));
  }
}
