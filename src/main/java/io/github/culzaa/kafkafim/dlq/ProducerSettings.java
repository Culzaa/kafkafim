package io.github.culzaa.kafkafim.dlq;

import io.github.culzaa.kafkafim.config.KafkaClientSecurity;
import io.github.culzaa.kafkafim.config.KafkaCluster;
import io.github.culzaa.kafkafim.config.OwnedKafkaKeys;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.jspecify.annotations.Nullable;

/**
 * Builds kafka-clients producer (and admin) settings for the DLQ producer of one {@code (cluster,
 * identity)} from the {@code config} model (DESIGN §5.1, §5.2.1).
 *
 * <p>Three layers, later ones winning: kafkafim's {@link #DEFAULTS}; the cluster's {@code
 * properties} passthrough, restricted to keys a producer knows (or that neither a producer nor a
 * consumer knows, such as plugin settings), because the same map also feeds the consumers; and
 * kafkafim's own settings. The passthrough may not set a key kafkafim owns: {@link #OWNED_KEYS}
 * always, plus the security keys derived from the cluster's {@code ssl} block or the identity's
 * {@code sasl}/{@code ssl} blocks when those blocks are present. Such a passthrough is rejected
 * with {@link IllegalArgumentException} naming only the keys, never the values.
 *
 * <p>{@code enable.idempotence} is the one durability-related key the passthrough may set, to
 * {@code true} (the default) or {@code false}; anything else is rejected. An idempotent producer
 * asks for a producer id ({@code InitProducerId}) before its first send, which some brokers
 * authorize only with the cluster-level {@code IdempotentWrite} ACL, not the {@code Write} ACL on
 * the DLQ topic. Without idempotence {@code max.in.flight.requests.per.connection} becomes 1 so
 * retries keep their order, and {@code acks=all} and {@code retries} stay. A retry after a lost
 * acknowledgement may then write a duplicate DLQ record; that is harmless, since replay is
 * idempotent by {@code _id} (ADR-0006). No other key is unlocked.
 *
 * <p>The security keys come from {@link KafkaClientSecurity}, which the consumers share.
 */
public final class ProducerSettings {

  /** Fixed: every DLQ record is acknowledged by all in-sync replicas. */
  static final String ACKS = "all";

  /** Fixed: kafka-clients' retries of one send stop after this; kafkafim then resends. */
  static final int DELIVERY_TIMEOUT_MS = 120_000;

  /** The idempotent producer's upper bound, so its internal retries keep the order. */
  static final int MAX_IN_FLIGHT = 5;

  /** Used when idempotence is switched off: a single in-flight request keeps retries in order. */
  static final int MAX_IN_FLIGHT_WITHOUT_IDEMPOTENCE = 1;

  /**
   * Keys the passthrough can never set: durability and serialization of DLQ records depend on them,
   * or kafkafim sets them per {@code (cluster, identity)}. {@code transactional.id} is owned so
   * that it stays unset: a transactional producer would need fencing kafkafim does not do; {@code
   * interceptor.classes} likewise, since an interceptor could change DLQ records.
   */
  public static final Set<String> OWNED_KEYS = OwnedKafkaKeys.PRODUCER;

  /**
   * Defaults the passthrough may override.
   *
   * <ul>
   *   <li>{@code compression.type=lz4}: cheap, and compression changes neither order nor
   *       durability.
   *   <li>{@code max.request.size} of 16 MiB: a record up to twice the default {@code
   *       max-record-bytes} plus kafkafim's headers fits; the DLQ topic's {@code max.message.bytes}
   *       must allow it too.
   *   <li>{@code max.block.ms} of 10s: bounds how long one send attempt waits for metadata or
   *       buffer space before kafkafim retries it.
   * </ul>
   */
  public static final Map<String, Object> DEFAULTS =
      Map.of(
          ProducerConfig.COMPRESSION_TYPE_CONFIG,
          "lz4",
          ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
          16 * 1024 * 1024,
          ProducerConfig.MAX_BLOCK_MS_CONFIG,
          10_000);

  private static final String CLIENT_ID_PREFIX = "kafkafim-dlq.";
  private static final String ADMIN_CLIENT_ID_PREFIX = "kafkafim-dlq-admin.";

  private ProducerSettings() {}

  /**
   * The DLQ producer's {@code client.id}: {@code kafkafim-dlq.<cluster>.<identity>}, or {@code
   * kafkafim-dlq.<cluster>} without an identity. Cluster and identity names are {@code [a-z0-9-]}
   * and never contain a dot, so the dot keeps {@code (a-b, c)} and {@code (a, b-c)} distinct:
   * producers of different pairs share neither an app-info MBean nor a metric series.
   *
   * @param clusterName the Kafka cluster's name
   * @param identityName the identity's name, or {@code null}
   * @return the client id
   */
  public static String clientId(String clusterName, @Nullable String identityName) {
    return CLIENT_ID_PREFIX + clusterName + (identityName == null ? "" : "." + identityName);
  }

  /**
   * The producer settings for one {@code (cluster, identity)}.
   *
   * @param clusterName the Kafka cluster's name
   * @param cluster the Kafka cluster
   * @param identityName the identity's name, or {@code null} when the pipeline resolves none (only
   *     valid on a cluster that does not require SASL, which the config validator guarantees)
   * @param identity the identity, or {@code null}
   * @return mutable settings map
   * @throws IllegalArgumentException if the passthrough sets a key kafkafim owns, or {@code
   *     enable.idempotence} to anything but {@code true} or {@code false}
   */
  public static Map<String, Object> forIdentity(
      String clusterName,
      KafkaCluster cluster,
      @Nullable String identityName,
      KafkaCluster.@Nullable KafkaIdentity identity) {
    Map<String, Object> owned = new LinkedHashMap<>();
    owned.put(
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, String.join(",", cluster.bootstrapServers()));
    owned.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, cluster.securityProtocol().name());
    owned.put(ProducerConfig.CLIENT_ID_CONFIG, clientId(clusterName, identityName));
    owned.put(ProducerConfig.ACKS_CONFIG, ACKS);
    boolean idempotent = idempotence(clusterName, cluster);
    owned.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idempotent);
    owned.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
    owned.put(
        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
        idempotent ? MAX_IN_FLIGHT : MAX_IN_FLIGHT_WITHOUT_IDEMPOTENCE);
    owned.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, DELIVERY_TIMEOUT_MS);
    owned.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    owned.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    owned.putAll(KafkaClientSecurity.properties(cluster, identity));

    Set<String> rejected = new TreeSet<>();
    for (String key : cluster.properties().keySet()) {
      if (key.equals(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)) {
        continue; // validated above; the owned map carries the resulting value
      }
      if (OWNED_KEYS.contains(key) || owned.containsKey(key)) {
        rejected.add(key);
      }
    }
    if (!rejected.isEmpty()) {
      throw new IllegalArgumentException(
          "Kafka cluster '"
              + clusterName
              + "' properties must not set keys the DLQ producer owns: "
              + rejected);
    }
    Map<String, Object> settings = new LinkedHashMap<>(DEFAULTS);
    cluster
        .properties()
        .forEach((key, value) -> {
          if (isProducerKey(key)) {
            settings.put(key, value);
          }
        });
    settings.putAll(owned);
    return settings;
  }

  /**
   * Admin settings for the DLQ topic check, derived from producer settings: the keys an admin
   * client knows, with the client id's {@code kafkafim-dlq.} prefix replaced by {@code
   * kafkafim-dlq-admin.} so it never equals a producer's id (an {@code -admin} or {@code .admin}
   * suffix could, for an identity of that name).
   *
   * @param producerSettings settings from {@link #forIdentity}
   * @return mutable admin settings
   */
  public static Map<String, Object> adminSettings(Map<String, Object> producerSettings) {
    Set<String> adminKeys = AdminClientConfig.configNames();
    Map<String, Object> admin = new LinkedHashMap<>();
    producerSettings.forEach((key, value) -> {
      if (adminKeys.contains(key)) {
        admin.put(key, value);
      }
    });
    String producerId = String.valueOf(producerSettings.get(ProducerConfig.CLIENT_ID_CONFIG));
    admin.put(
        AdminClientConfig.CLIENT_ID_CONFIG,
        producerId.startsWith(CLIENT_ID_PREFIX)
            ? ADMIN_CLIENT_ID_PREFIX + producerId.substring(CLIENT_ID_PREFIX.length())
            : producerId + "-admin");
    return admin;
  }

  /**
   * A printable copy of settings: values of keys that may carry secrets are replaced, so the result
   * is safe to log.
   *
   * @param settings producer or admin settings
   * @return the settings with secret values masked
   */
  public static Map<String, Object> redacted(Map<String, ?> settings) {
    return KafkaClientSecurity.redacted(settings);
  }

  /** The passthrough's {@code enable.idempotence}: {@code true} when unset. */
  private static boolean idempotence(String clusterName, KafkaCluster cluster) {
    String value = cluster.properties().get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG);
    if (value == null || value.equals("true")) {
      return true;
    }
    if (value.equals("false")) {
      return false;
    }
    throw new IllegalArgumentException(
        "Kafka cluster '"
            + clusterName
            + "' property "
            + ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG
            + " must be true or false");
  }

  /** Producer keys, plus keys neither client knows (plugins); consumer-only keys are dropped. */
  private static boolean isProducerKey(String key) {
    return ProducerConfig.configNames().contains(key)
        || !ConsumerConfig.configNames().contains(key);
  }
}
