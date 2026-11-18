package io.github.culzaa.kafkafim.dlq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.culzaa.kafkafim.KafkaTestContainers;
import io.github.culzaa.kafkafim.KafkaTopics;
import io.github.culzaa.kafkafim.config.KafkaCluster;
import io.github.culzaa.kafkafim.config.KafkafimProperties;
import io.github.culzaa.kafkafim.config.Pipeline;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * {@link DlqWriter} with {@code enable.idempotence=false} from the cluster {@code properties}
 * against a real broker: the writer's guarantees (completion only after the acknowledgement, no
 * loss across a broker outage, at-least-once; no order is promised, DESIGN §9) hold without the
 * idempotent producer, because they rest on {@code acks=all} and retries: kafka-clients' own, and
 * kafkafim's loop once those expire. The outage here is shorter than {@code delivery.timeout.ms},
 * so it exercises the former (DESIGN §7, §9).
 */
@Testcontainers
class DlqWriterNoIdempotenceIntegrationTest {

  @Container static final KafkaContainer KAFKA = KafkaTestContainers.kafka();

  private static final Duration TIMEOUT = Duration.ofSeconds(90);
  private static final DlqOptions OPTIONS =
      new DlqOptions(
          Duration.ofMillis(50),
          Duration.ofMillis(500),
          Duration.ofSeconds(5),
          Duration.ofSeconds(5));

  private static Admin admin;

  private final List<DlqFailure> failures = new CopyOnWriteArrayList<>();
  private DlqProducers producers;

  @BeforeAll
  static void connect() {
    admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()));
  }

  @AfterAll
  static void disconnect() {
    admin.close();
  }

  @AfterEach
  void closeProducers() {
    if (KAFKA
            .getDockerClient()
            .inspectContainerCmd(KAFKA.getContainerId())
            .exec()
            .getState()
            .getPaused()
        == Boolean.TRUE) {
      KAFKA.getDockerClient().unpauseContainerCmd(KAFKA.getContainerId()).exec();
    }
    if (producers != null) {
      producers.close();
    }
  }

  private DlqWriter writer() {
    KafkaCluster local =
        new KafkaCluster(
            List.of(KAFKA.getBootstrapServers()),
            null,
            null,
            null,
            null,
            Map.of("enable.idempotence", "false", "request.timeout.ms", "5000"));
    Pipeline pipeline =
        new Pipeline(
            new Pipeline.Source(
                "local",
                List.of("orders-" + UUID.randomUUID()),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null),
            null,
            null,
            null,
            null,
            null);
    producers =
        new DlqProducers(
            new KafkafimProperties(
                false,
                new KafkafimProperties.Kafka(Map.of("local", local)),
                null,
                Map.of("orders", pipeline)),
            new DlqListener() {
              @Override
              public void onSendFailed(String pipeline, DlqRequest request, DlqFailure failure) {
                failures.add(failure);
              }
            },
            OPTIONS);
    return producers.writer("orders");
  }

  private static DlqRequest request(int i) {
    return new DlqRequest(
        "orders.v2",
        0,
        i,
        null,
        null,
        ("doc-" + i).getBytes(StandardCharsets.UTF_8),
        List.of(),
        "decode",
        "bad " + i,
        null,
        null,
        1,
        Instant.now(),
        null);
  }

  /** Values on the topic in log order, duplicates included, once at least {@code min} distinct. */
  private static List<String> readValues(String topic, int min) {
    try (KafkaConsumer<byte[], byte[]> consumer =
        new KafkaConsumer<>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA.getBootstrapServers(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class))) {
      TopicPartition partition = new TopicPartition(topic, 0);
      consumer.assign(List.of(partition));
      consumer.seekToBeginning(List.of(partition));
      List<String> values = new ArrayList<>();
      long deadline = System.nanoTime() + TIMEOUT.toNanos();
      while (System.nanoTime() < deadline) {
        for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(500))) {
          values.add(new String(record.value(), StandardCharsets.UTF_8));
        }
        if (new LinkedHashSet<>(values).size() >= min) {
          return values;
        }
      }
      throw new AssertionError(
          "fewer than " + min + " distinct records on " + topic + ": " + values);
    }
  }

  @Test
  void acknowledgedRecordsLandOnTheTopicExactlyOnceWithoutFailures() throws Exception {
    DlqWriter writer = writer();
    KafkaTopics.create(admin, List.of(new NewTopic(writer.topic(), 1, (short) 1)));
    int count = 50;

    List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      futures.add(writer.send(request(i)));
    }
    Set<Long> offsets = new LinkedHashSet<>();
    for (CompletableFuture<RecordMetadata> future : futures) {
      offsets.add(future.get(60, TimeUnit.SECONDS).offset());
    }

    assertThat(offsets).hasSize(count);
    List<String> values = readValues(writer.topic(), count);
    // Not in send order: the writer sends concurrently and never promised an order (DESIGN §9).
    assertThat(values).hasSize(count).containsExactlyInAnyOrderElementsOf(expected(count));
    assertThat(failures).isEmpty();
  }

  @Test
  void brokerOutageLosesNothingAndCompletesOnlyAfterTheAcknowledgement() throws Exception {
    DlqWriter writer = writer();
    KafkaTopics.create(admin, List.of(new NewTopic(writer.topic(), 1, (short) 1)));
    writer.send(request(0)).get(60, TimeUnit.SECONDS); // warms the metadata

    KAFKA.getDockerClient().pauseContainerCmd(KAFKA.getContainerId()).exec();
    List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      futures.add(writer.send(request(i)));
    }
    Thread.sleep(15_000);
    assertThat(futures).allSatisfy(future -> assertThat(future).isNotDone());

    KAFKA.getDockerClient().unpauseContainerCmd(KAFKA.getContainerId()).exec();
    for (CompletableFuture<RecordMetadata> future : futures) {
      future.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
    }

    List<String> values = readValues(writer.topic(), 11);
    // Every acknowledged record is on the topic; duplicates (a retry after a lost acknowledgement)
    // and reordering are allowed.
    assertThat(new LinkedHashSet<>(values)).containsAll(expected(11));
    await().atMost(TIMEOUT).until(() -> !writer.isBlocked());
  }

  private static List<String> expected(int count) {
    List<String> out = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      out.add("doc-" + i);
    }
    return out;
  }
}
