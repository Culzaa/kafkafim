package io.github.culzaa.kafkafim.supervisor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.culzaa.kafkafim.KafkaTestContainers;
import io.github.culzaa.kafkafim.KafkaTopics;
import io.github.culzaa.kafkafim.config.KafkafimProperties;
import io.github.culzaa.kafkafim.decode.StepRegistry;
import io.github.culzaa.kafkafim.decode.TestSteps;
import io.github.culzaa.kafkafim.dlq.DlqOptions;
import io.github.culzaa.kafkafim.sink.es.BreakerSettings;
import io.github.culzaa.kafkafim.sink.es.ClientSettings;
import io.github.culzaa.kafkafim.source.kafka.SourceOptions;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.json.JsonMapper;

/**
 * Route transforms end to end (DESIGN §8, ADR-0047) against a real broker and Elasticsearch 9.5,
 * with the framework's test ops (the transform framework ships without a production op): a copy of
 * a transformed route reaches Elasticsearch with its rewritten bytes, a copy of a plain route keeps
 * the record's bytes, and a copy whose step fails is dead-lettered with the original value, its
 * target headers and {@code error.type=transform}, while its offset is committed.
 */
class TransformIntegrationTest {

  private static final KafkaContainer KAFKA =
      KafkaTestContainers.kafka().withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

  private static final ElasticsearchContainer ES =
      new ElasticsearchContainer(
              DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:9.5.6"))
          .withEnv("xpack.security.enabled", "false")
          .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m -XX:TieredStopAtLevel=1")
          // Parallel Failsafe forks can boot several nodes at once; the 60 s default is tight.
          .withStartupTimeout(Duration.ofMinutes(3));

  private static final Duration TIMEOUT = Duration.ofSeconds(90);
  private static final JsonMapper JSON = JsonMapper.builder().build();

  private static Admin admin;
  private static KafkaProducer<byte[], byte[]> producer;
  private static HttpClient http;

  private PipelineResources resources;
  private PipelineSupervisor supervisor;

  @BeforeAll
  static void startContainers() {
    Startables.deepStart(KAFKA, ES).join();
    admin = KafkaTopics.awaitedAdmin(KAFKA.getBootstrapServers());
    producer = new KafkaProducer<>(KafkaTopics.seedingProducerConfig(KAFKA.getBootstrapServers()));
    http = HttpClient.newHttpClient();
  }

  @AfterAll
  static void stopContainers() {
    if (producer != null) {
      producer.close();
    }
    if (admin != null) {
      admin.close();
    }
    if (http != null) {
      http.close();
    }
    ES.stop();
    KAFKA.stop();
  }

  @AfterEach
  void close() {
    if (supervisor != null) {
      supervisor.stop();
    }
    if (resources != null) {
      resources.close();
    }
  }

  @Test
  void transformedCopiesAreRewrittenPlainCopiesKeepTheirBytesAndFailuresAreDeadLettered()
      throws Exception {
    String id = "tx" + UUID.randomUUID().toString().substring(0, 8); // topic and index prefix
    String topic = id;
    String dlqTopic = topic + ".kafkafim-dlq";
    KafkaTopics.create(
        admin, List.of(new NewTopic(topic, 1, (short) 1), new NewTopic(dlqTopic, 1, (short) 1)));
    startPipeline(topic, id);

    // A noop step still rewrites: compact one line, exact number value, 1e3 spelled 1E+3.
    String eu = "{ \"id\": \"eu-1\",\n  \"region\": \"eu\", \"price\": 1.10, \"exp\": 1e3 }";
    String other = "{ \"id\": \"us-1\", \"region\": \"us\", \"exp\": 1e3 }";
    String bad = "{\"id\":\"bad-1\",\"region\":\"bad\",\"kind\":\"k\",\"a\":\"s\"}";
    send(topic, eu);
    send(topic, other);
    send(topic, bad);
    // An array record: each element is its own copy; the second one fails on its own.
    send(topic, "[" + eu.replace("eu-1", "eu-2") + "," + bad.replace("bad-1", "bad-2") + "]");

    await().atMost(TIMEOUT).untilAsserted(() -> assertThat(count(id + "-*")).isEqualTo(3));
    String transformed = source(id + "-eu", "eu-1");
    assertThat(transformed)
        .isEqualTo(
            "{\"id\":\"eu-1\",\"region\":\"eu\",\"price\":1.10,\"exp\":1E+3,\"stamp\":\"x\"}");
    assertThat(source(id + "-eu", "eu-2")).contains("\"stamp\":\"x\"");
    assertThat(source(id + "-other", "us-1")).contains("\"exp\": 1e3").doesNotContain("stamp");

    List<ConsumerRecord<byte[], byte[]>> dead = awaitRecords(dlqTopic, 2);
    assertThat(dead)
        .allSatisfy(r -> {
          assertThat(header(r, "kafkafim.error.type")).isEqualTo("transform");
          assertThat(header(r, "kafkafim.target.cluster")).isEqualTo("es");
          assertThat(header(r, "kafkafim.target.index")).isEqualTo(id + "-bad-k");
          assertThat(header(r, "kafkafim.error.reason"))
              .startsWith("route bad, step 1 (fail /a/b): ")
              .doesNotContain("bad-");
        });
    // The DLQ value is the original record value, an array element's record included.
    assertThat(dead.stream().map(r -> new String(r.value(), StandardCharsets.UTF_8)))
        .containsExactlyInAnyOrder(
            bad, "[" + eu.replace("eu-1", "eu-2") + "," + bad.replace("bad-1", "bad-2") + "]");
    assertThat(dead.stream().map(r -> header(r, "kafkafim.doc-index")))
        .containsExactlyInAnyOrder("", "1");
    assertThat(count(id + "-bad-k")).isZero();

    await()
        .atMost(TIMEOUT)
        .untilAsserted(() ->
            assertThat(
                    admin
                        .listConsumerGroupOffsets("kafkafim.p")
                        .partitionsToOffsetAndMetadata()
                        .get()
                        .entrySet()
                        .stream()
                        .filter(e -> e.getKey().topic().equals(topic))
                        .mapToLong(e -> e.getValue().offset())
                        .sum())
                .isEqualTo(4));
  }

  @Test
  void removeStripsFieldsFromItsCopyAfterRoutingAndIdWhileTheSiblingKeepsTheRecord()
      throws Exception {
    String id = "rm" + UUID.randomUUID().toString().substring(0, 8);
    KafkaTopics.create(
        admin,
        List.of(new NewTopic(id, 1, (short) 1), new NewTopic(id + ".kafkafim-dlq", 1, (short) 1)));
    start(
        TestConfigs.bind(
            """
            kafkafim:
              kafka:
                clusters:
                  k: { bootstrap-servers: ["%s"] }
              elasticsearch:
                clusters:
                  es: { uris: ["http://%s"] }
              pipelines:
                p:
                  source: { cluster: k, topics: [%s], offset-reset: earliest }
                  route-mode: all
                  routes:
                    - name: full
                      when: [ { field: /region, equals: eu } ]
                      cluster: es
                      index: "%s-full"
                    - name: stripped
                      when: [ { field: /region, equals: eu } ]
                      cluster: es
                      index: "%s-stripped-%%{kind}"
                      transform:
                        - { op: remove, field: /card/number }
                        - { op: remove, field: /id }
                        - { op: remove, field: /region }
                        - { op: remove, field: /kind }
                        - { op: remove, field: /nothing/here }
            """
                .formatted(KAFKA.getBootstrapServers(), ES.getHttpHostAddress(), id, id, id)),
        StepRegistry.BUILT_IN);

    String doc =
        "{ \"id\": \"r-1\", \"region\": \"eu\", \"kind\": \"k\","
            + " \"card\": { \"number\": \"4111\", \"brand\": \"v\" }, \"n\": null }";
    send(id, doc);

    await().atMost(TIMEOUT).untilAsserted(() -> assertThat(count(id + "-*")).isEqualTo(2));
    assertThat(source(id + "-full", "r-1")).isEqualTo(doc);
    assertThat(source(id + "-stripped-k", "r-1"))
        .isEqualTo("{\"card\":{\"brand\":\"v\"},\"n\":null}");
  }

  @Test
  void renameMovesFieldsInItsCopyAfterRoutingAndIdWhileTheSiblingKeepsTheRecord() throws Exception {
    String id = "rn" + UUID.randomUUID().toString().substring(0, 8);
    KafkaTopics.create(
        admin,
        List.of(new NewTopic(id, 1, (short) 1), new NewTopic(id + ".kafkafim-dlq", 1, (short) 1)));
    start(
        TestConfigs.bind(
            """
            kafkafim:
              kafka:
                clusters:
                  k: { bootstrap-servers: ["%s"] }
              elasticsearch:
                clusters:
                  es: { uris: ["http://%s"] }
              pipelines:
                p:
                  source: { cluster: k, topics: [%s], offset-reset: earliest }
                  route-mode: all
                  routes:
                    - name: full
                      when: [ { field: /region, equals: eu } ]
                      cluster: es
                      index: "%s-full"
                    - name: renamed
                      when: [ { field: /region, equals: eu } ]
                      cluster: es
                      index: "%s-renamed-%%{kind}"
                      transform:
                        - { op: rename, field: /usr, to: /user }
                        - { op: rename, field: /kind, to: /meta/kind }
                        - { op: rename, field: /nothing, to: /here }
            """
                .formatted(KAFKA.getBootstrapServers(), ES.getHttpHostAddress(), id, id, id)),
        StepRegistry.BUILT_IN);

    String doc =
        "{ \"id\": \"r-1\", \"region\": \"eu\", \"kind\": \"k\", \"usr\": null,"
            + " \"ratio\": 0.10 }";
    send(id, doc);

    await().atMost(TIMEOUT).untilAsserted(() -> assertThat(count(id + "-*")).isEqualTo(2));
    assertThat(source(id + "-full", "r-1")).isEqualTo(doc);
    assertThat(source(id + "-renamed-k", "r-1"))
        .isEqualTo(
            "{\"id\":\"r-1\",\"region\":\"eu\",\"ratio\":0.10,"
                + "\"user\":null,\"meta\":{\"kind\":\"k\"}}");
  }

  @Test
  void removeRenameAndSetRunInOrderOnOneRoute() throws Exception {
    String id = "all" + UUID.randomUUID().toString().substring(0, 8);
    KafkaTopics.create(
        admin,
        List.of(new NewTopic(id, 1, (short) 1), new NewTopic(id + ".kafkafim-dlq", 1, (short) 1)));
    start(
        TestConfigs.bind(
            """
            kafkafim:
              kafka:
                clusters:
                  k: { bootstrap-servers: ["%s"] }
              elasticsearch:
                clusters:
                  es: { uris: ["http://%s"] }
              pipelines:
                p:
                  source: { cluster: k, topics: [%s], offset-reset: earliest }
                  routes:
                    - name: eu
                      when: [ { field: /region, equals: eu } ]
                      cluster: es
                      index: "%s-%%{region}"
                      transform:
                        - { op: remove, field: /card/number }
                        - { op: rename, field: /usr, to: /user }
                        - { op: set, field: /event/dataset, value: orders }
                        - { op: set, field: /event/version, json: "2" }
                        - { op: rename, field: /event/version, to: /event/v }
            """
                .formatted(KAFKA.getBootstrapServers(), ES.getHttpHostAddress(), id, id)),
        StepRegistry.BUILT_IN);

    send(
        id,
        "{ \"id\": \"o-1\", \"region\": \"eu\", \"usr\": \"ann\","
            + " \"card\": { \"number\": \"4111\", \"brand\": \"v\" }, \"total\": 12.50 }");

    await().atMost(TIMEOUT).untilAsserted(() -> assertThat(count(id + "-*")).isEqualTo(1));
    assertThat(source(id + "-eu", "o-1"))
        .isEqualTo(
            "{\"id\":\"o-1\",\"region\":\"eu\",\"card\":{\"brand\":\"v\"},\"total\":12.50,"
                + "\"user\":\"ann\",\"event\":{\"dataset\":\"orders\",\"v\":2}}");
  }

  private void startPipeline(String topic, String id) {
    KafkafimProperties properties =
        TestConfigs.bind(
            """
            kafkafim:
              kafka:
                clusters:
                  k: { bootstrap-servers: ["%s"] }
              elasticsearch:
                clusters:
                  es: { uris: ["http://%s"] }
              pipelines:
                p:
                  source: { cluster: k, topics: [%s], offset-reset: earliest }
                  routes:
                    - name: eu
                      when: [ { field: /region, equals: eu } ]
                      cluster: es
                      index: "%s-eu"
                      transform: [ { op: noop }, { op: append, field: stamp, value: x } ]
                    - name: bad
                      when: [ { field: /region, equals: bad } ]
                      cluster: es
                      index: "%s-bad-%%{kind}"
                      transform: [ { op: fail, field: /a/b } ]
                    - { name: other, default: true, cluster: es, index: "%s-other" }
            """
                .formatted(
                    KAFKA.getBootstrapServers(), ES.getHttpHostAddress(), topic, id, id, id));
    start(properties, TestSteps.REGISTRY);
  }

  private void start(KafkafimProperties properties, StepRegistry steps) {
    ListenerRelay relay = new ListenerRelay(List.of(), List.of());
    resources =
        new PipelineResources(
            relay,
            p -> (cluster, identity) -> Preflight.Status.OK,
            new PipelineResources.Settings(
                SourceOptions.defaults(),
                Duration.ofSeconds(1),
                DlqOptions.DEFAULTS,
                ClientSettings.DEFAULT,
                BreakerSettings.DEFAULT,
                Clock.systemUTC(),
                steps));
    supervisor = new PipelineSupervisor(properties, resources, List.of());
    relay.attach(supervisor);
    supervisor.start();
    await().atMost(TIMEOUT).until(() -> supervisor.state("p") == PipelineState.RUNNING);
  }

  private static void send(String topic, String value) throws Exception {
    producer
        .send(
            new ProducerRecord<byte[], byte[]>(topic, null, value.getBytes(StandardCharsets.UTF_8)))
        .get();
  }

  private static List<ConsumerRecord<byte[], byte[]>> awaitRecords(String topic, int expected) {
    List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
    await()
        .atMost(TIMEOUT)
        .untilAsserted(() -> {
          records.clear();
          Properties config = new Properties();
          config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
          config.put(ConsumerConfig.GROUP_ID_CONFIG, "it-reader-" + UUID.randomUUID());
          config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
          config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
          config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
          try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
            while (System.nanoTime() < deadline) {
              consumer.poll(Duration.ofMillis(200)).forEach(records::add);
            }
          }
          assertThat(records).hasSize(expected);
        });
    return records;
  }

  private static String header(ConsumerRecord<byte[], byte[]> record, String name) {
    Header header = record.headers().lastHeader(name);
    return header == null ? "" : new String(header.value(), StandardCharsets.UTF_8);
  }

  private static HttpResponse<String> es(String method, String path) throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder(URI.create("http://" + ES.getHttpHostAddress() + path))
            .header("Content-Type", "application/json")
            .method(method, HttpRequest.BodyPublishers.noBody())
            .build();
    return http.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private static long count(String indices) throws Exception {
    es("POST", "/" + indices + "/_refresh?ignore_unavailable=true&allow_no_indices=true");
    HttpResponse<String> response =
        es("GET", "/" + indices + "/_count?ignore_unavailable=true&allow_no_indices=true");
    return response.statusCode() == 200 ? JSON.readTree(response.body()).get("count").asLong() : 0;
  }

  /** The {@code _source} of a document, as the text Elasticsearch stored it. */
  private static String source(String index, String id) throws Exception {
    String body = es("GET", "/" + index + "/_doc/" + id).body();
    assertThat(JSON.readTree(body).path("found").asBoolean()).isTrue();
    int start = body.indexOf("\"_source\":") + "\"_source\":".length();
    return body.substring(start, body.length() - 1); // _source is the last member
  }
}
