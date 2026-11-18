package io.github.matabar.kafkafim.config;

import java.util.Set;

/**
 * The kafka-clients keys the {@code properties} passthrough of a Kafka cluster may never set,
 * because kafkafim owns them (DESIGN §5.2.1). One source for the bind-time check ({@link
 * ConfigValidator}) and for the settings builders of the consumers ({@code source.kafka}) and the
 * DLQ producers ({@code dlq}), which may not depend on each other (ADR-0003).
 *
 * <p>The keys are written out because {@code config} does not depend on kafka-clients; a test in
 * each builder's package pins every one to its {@code ConsumerConfig}/{@code ProducerConfig}
 * constant. The security keys kafkafim derives per identity are not listed: they follow from the
 * cluster's blocks ({@link KafkaClientSecurity#properties}).
 */
public final class OwnedKafkaKeys {

  /**
   * Keys the consumer passthrough can never set: correctness of offsets, assignment and
   * deserialization depends on them, or they are set per pipeline from the {@code source} block.
   */
  public static final Set<String> CONSUMER =
      Set.of(
          "bootstrap.servers",
          "security.protocol",
          "group.id",
          "group.instance.id",
          "group.protocol",
          "client.id",
          "enable.auto.commit",
          "auto.offset.reset",
          "max.poll.records",
          "key.deserializer",
          "value.deserializer",
          "partition.assignment.strategy",
          "allow.auto.create.topics");

  /**
   * Keys the DLQ producer passthrough can never set: durability and serialization of DLQ records
   * depend on them, or kafkafim sets them per {@code (cluster, identity)}. {@code transactional.id}
   * stays unset (a transactional producer needs fencing kafkafim does not do), and so does {@code
   * interceptor.classes} (an interceptor could change DLQ records). {@code enable.idempotence} is
   * not owned: it may be set to {@code true} or {@code false}.
   */
  public static final Set<String> PRODUCER =
      Set.of(
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
          "interceptor.classes");

  private OwnedKafkaKeys() {}
}
