package io.github.matabar.kafkafim.decode;

import static io.github.matabar.kafkafim.GeneratedInputs.ROUNDS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.matabar.kafkafim.GeneratedInputs;
import io.github.matabar.kafkafim.decode.DecodeSpec.IdMissing;
import io.github.matabar.kafkafim.decode.DecodeSpec.OnBadTimestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

/** Generated inputs for {@link Decoder} and {@link TimestampFormat}: never throws, round trips. */
class DecoderGeneratedInputTest {

  private static final Instant INGEST = Instant.parse("2026-11-07T12:00:00Z");
  private static final JsonMapper JSON = JsonMapper.builder().build();
  private static final Decoder LENIENT =
      Decoder.compile(
          new DecodeSpec(
              "/id", IdMissing.AUTO, "/v", "/ts", "iso-8601", OnBadTimestamp.INGEST_TIME, 1 << 16));
  private static final Decoder STRICT =
      Decoder.compile(
          new DecodeSpec("/id", IdMissing.DLQ, null, null, "iso-8601", OnBadTimestamp.DLQ, 64));

  @Test
  void arbitraryBytesNeverThrow() {
    Random random = new Random(303);
    for (int i = 0; i < ROUNDS; i++) {
      byte[] value = GeneratedInputs.bytes(random, 200);
      assertThat(LENIENT.decode(value, INGEST)).as("round %d", i).isNotNull();
      assertThat(STRICT.decode(value, INGEST)).as("round %d", i).isNotNull();
    }
  }

  @Test
  void mutatedValidJsonNeverThrows() {
    Random random = new Random(304);
    for (int i = 0; i < ROUNDS; i++) {
      byte[] value = document(random, true).toString().getBytes(UTF_8);
      switch (random.nextInt(3)) {
        case 0 -> value[random.nextInt(value.length)] = (byte) random.nextInt(256);
        case 1 -> value = Arrays.copyOf(value, random.nextInt(value.length));
        default -> {
          byte[] longer = Arrays.copyOf(value, value.length + 1);
          longer[value.length] = (byte) random.nextInt(256);
          value = longer;
        }
      }
      if (value.length > 0) {
        assertThat(LENIENT.decode(value, INGEST)).as("round %d", i).isNotNull();
      }
    }
  }

  @Test
  void objectRoundTripsThroughTheDecoder() {
    Random random = new Random(305);
    for (int i = 0; i < ROUNDS; i++) {
      ObjectNode original = document(random, false);
      DecodeResult result = LENIENT.decode(original.toString().getBytes(UTF_8), INGEST);

      assertThat(result).as("round %d: %s", i, original).isInstanceOf(DecodeResult.Documents.class);
      DecodeResult.Documents documents = (DecodeResult.Documents) result;
      assertThat(documents.array()).isFalse();
      assertThat(documents.outcomes()).hasSize(1);
      DocumentOutcome.Routable routable = (DocumentOutcome.Routable) documents.outcomes().get(0);
      assertThat(routable.node()).as("round %d", i).isEqualTo(original);
    }
  }

  @Test
  void arrayRoundTripsOneOutcomePerElement() {
    Random random = new Random(306);
    for (int i = 0; i < ROUNDS / 10; i++) {
      ArrayNode original = JSON.createArrayNode();
      int size = 1 + random.nextInt(8);
      for (int e = 0; e < size; e++) {
        original.add(document(random, false));
      }
      DecodeResult result = LENIENT.decode(original.toString().getBytes(UTF_8), INGEST);

      DecodeResult.Documents documents = (DecodeResult.Documents) result;
      assertThat(documents.array()).isTrue();
      assertThat(documents.outcomes()).hasSize(size);
      for (int e = 0; e < size; e++) {
        assertThat(documents.outcomes().get(e).index()).isEqualTo(e);
        assertThat(((DocumentOutcome.Routable) documents.outcomes().get(e)).node())
            .isEqualTo(original.get(e));
      }
    }
  }

  @Test
  void oversizedValueFailsWithoutParsing() {
    Random random = new Random(307);
    for (int i = 0; i < 200; i++) {
      byte[] value = new byte[65 + random.nextInt(500)];
      random.nextBytes(value);
      assertThat(STRICT.decode(value, INGEST)).isInstanceOf(DecodeResult.RecordFailure.class);
    }
  }

  @Test
  void everyTimestampFormatNeverThrowsOnAnyJsonValue() {
    List<TimestampFormat> formats =
        List.of(
            TimestampFormat.compile("iso-8601"),
            TimestampFormat.compile("epoch-millis"),
            TimestampFormat.compile("epoch-seconds"),
            TimestampFormat.compile("pattern:yyyy-MM-dd HH:mm:ss"),
            TimestampFormat.compile("pattern:dd/MMM/yyyy:HH:mm:ss Z"));
    Random random = new Random(308);
    for (int i = 0; i < ROUNDS; i++) {
      JsonNode value = scalarOrContainer(random);
      for (TimestampFormat format : formats) {
        TimestampFormat.Parse parse = format.parse(value);
        assertThat(parse.instant() != null ^ parse.error() != null)
            .as("exactly one of instant/error for %s", value)
            .isTrue();
      }
    }
  }

  @Test
  void epochAndIsoRoundTrip() {
    TimestampFormat millis = TimestampFormat.compile("epoch-millis");
    TimestampFormat seconds = TimestampFormat.compile("epoch-seconds");
    TimestampFormat iso = TimestampFormat.compile("iso-8601");
    Random random = new Random(309);
    for (int i = 0; i < ROUNDS; i++) {
      long epochSecond = -62_135_596_800L + (long) (random.nextDouble() * 253_402_300_000L);
      Instant instant = Instant.ofEpochSecond(epochSecond, random.nextInt(1000) * 1_000_000L);

      assertThat(millis.parse(JSON.valueToTree(instant.toEpochMilli())).instant())
          .as("millis %s", instant)
          .isEqualTo(instant);
      assertThat(seconds.parse(JSON.valueToTree(instant.getEpochSecond())).instant())
          .isEqualTo(Instant.ofEpochSecond(instant.getEpochSecond()));
      assertThat(iso.parse(JSON.valueToTree(instant.toString())).instant())
          .as("iso %s", instant)
          .isEqualTo(instant);
    }
  }

  @Test
  void invalidFormatsAreRejectedWithIllegalArgumentOnly() {
    Random random = new Random(310);
    for (int i = 0; i < ROUNDS; i++) {
      String format =
          random.nextBoolean()
              ? GeneratedInputs.text(random, 12, true)
              : "pattern:" + GeneratedInputs.text(random, 12, true);
      try {
        TimestampFormat.compile(format);
      } catch (IllegalArgumentException expected) {
        // a rejected format is the documented outcome
      }
    }
  }

  /** A random document with a string id and, unless {@code broken}, valid optional fields. */
  private static ObjectNode document(Random random, boolean broken) {
    ObjectNode node = JSON.createObjectNode();
    node.put("id", GeneratedInputs.text(random, 20, false));
    if (random.nextBoolean()) {
      node.put("v", random.nextLong(1, Long.MAX_VALUE));
    }
    if (random.nextBoolean()) {
      node.put("ts", "2026-11-0" + (1 + random.nextInt(9)) + "T10:15:30Z");
    }
    int fields = random.nextInt(5);
    for (int f = 0; f < fields; f++) {
      String key = "k" + f;
      switch (random.nextInt(5)) {
        case 0 -> node.put(key, GeneratedInputs.text(random, 30, false));
        case 1 -> node.put(key, random.nextDouble());
        case 2 -> node.put(key, random.nextBoolean());
        case 3 -> node.putNull(key);
        default ->
            node.putArray(key).add(random.nextInt()).add(GeneratedInputs.text(random, 5, false));
      }
    }
    if (broken && random.nextInt(4) == 0) {
      node.put("id", random.nextInt());
    }
    return node;
  }

  private static JsonNode scalarOrContainer(Random random) {
    return switch (random.nextInt(8)) {
      case 0 -> JSON.valueToTree(random.nextLong());
      case 1 -> JSON.valueToTree(random.nextDouble() * 1e30);
      case 2 -> JSON.valueToTree(random.nextBoolean());
      case 3 -> JSON.nullNode();
      case 4 -> JSON.createArrayNode().add(1);
      case 5 -> JSON.createObjectNode().put("a", 1);
      case 6 -> JSON.valueToTree("2026-" + random.nextInt(99) + "-" + random.nextInt(99));
      default -> JSON.valueToTree(GeneratedInputs.text(random, 40, false));
    };
  }
}
