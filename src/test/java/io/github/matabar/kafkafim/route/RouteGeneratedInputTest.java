package io.github.matabar.kafkafim.route;

import static io.github.matabar.kafkafim.GeneratedInputs.ROUNDS;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.matabar.kafkafim.GeneratedInputs;
import io.github.matabar.kafkafim.route.ConditionSpec.FieldEquals;
import io.github.matabar.kafkafim.route.ConditionSpec.FieldExists;
import io.github.matabar.kafkafim.route.ConditionSpec.FieldIn;
import io.github.matabar.kafkafim.route.ConditionSpec.FieldMatches;
import io.github.matabar.kafkafim.route.ConditionSpec.FieldPrefix;
import io.github.matabar.kafkafim.route.ConditionSpec.HeaderEquals;
import io.github.matabar.kafkafim.route.ConditionSpec.TopicEquals;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Generated inputs for {@link IndexPattern} and {@link Condition}: never throw on content. */
class RouteGeneratedInputTest {

  private static final JsonMapper JSON = JsonMapper.builder().build();
  private static final Instant EVENT = Instant.parse("2026-11-07T12:00:00Z");

  @Test
  void compileOnlyEverThrowsIllegalArgument() {
    Random random = new Random(311);
    String alphabet = "%{}+/abc-_ .yMd~0";
    for (int i = 0; i < ROUNDS * 5; i++) {
      StringBuilder pattern = new StringBuilder();
      int length = random.nextInt(14);
      for (int c = 0; c < length; c++) {
        pattern.append(alphabet.charAt(random.nextInt(alphabet.length())));
      }
      try {
        IndexPattern.compile(pattern.toString());
      } catch (IllegalArgumentException expected) {
        // rejected configuration is the documented outcome
      }
    }
  }

  @Test
  void resolveNeverThrowsForAnyDocumentContent() {
    IndexPattern pattern = IndexPattern.compile("logs-%{/a}-%{b}-%{/c/d}-%{+yyyy.MM.dd}");
    Random random = new Random(312);
    for (int i = 0; i < ROUNDS; i++) {
      IndexResolution resolution = pattern.resolve(document(random), EVENT);
      assertThat(resolution).as("round %d", i).isNotNull();
      if (resolution instanceof IndexResolution.Resolved resolved) {
        assertThat(resolved.index()).isEqualTo(resolved.index().toLowerCase(java.util.Locale.ROOT));
        assertThat(IndexNames.problem(resolved.index())).isNull();
      }
    }
  }

  @Test
  void literalPatternsResolveToThemselves() {
    Random random = new Random(313);
    for (int i = 0; i < ROUNDS; i++) {
      StringBuilder name = new StringBuilder("idx");
      int length = random.nextInt(20);
      for (int c = 0; c < length; c++) {
        name.append("abcdefghijklmnopqrstuvwxyz0123456789-_".charAt(random.nextInt(38)));
      }
      assertThat(IndexPattern.compile(name.toString()).resolve(JSON.createObjectNode(), EVENT))
          .isEqualTo(new IndexResolution.Resolved(name.toString()));
    }
  }

  @Test
  void conditionsNeverThrowOnAnyDocumentTopicOrHeader() {
    Random random = new Random(314);
    List<Condition> conditions =
        List.of(
            Condition.compile(new FieldEquals("/a", "x")),
            Condition.compile(new FieldIn("/a", List.of("1", "true", "x"))),
            Condition.compile(new FieldExists("/c/d", true)),
            Condition.compile(new FieldPrefix("/a", "pre")),
            Condition.compile(new FieldMatches("/a", "(a|b)*c.*")),
            Condition.compile(new HeaderEquals("h", "v")),
            Condition.compile(new TopicEquals("t")));
    for (int i = 0; i < ROUNDS; i++) {
      JsonNode document = document(random);
      String topic = GeneratedInputs.text(random, 10, true);
      HeaderLookup headers =
          HeaderLookup.of(
              Map.of("h", GeneratedInputs.text(random, 4, false).getBytes(StandardCharsets.UTF_8)));
      for (Condition condition : conditions) {
        condition.test(document, topic, headers);
        condition.test(document, topic, HeaderLookup.NONE);
      }
    }
  }

  @Test
  void conditionCompileOnlyEverThrowsIllegalArgument() {
    Random random = new Random(315);
    for (int i = 0; i < ROUNDS; i++) {
      String junk = GeneratedInputs.text(random, 10, true);
      for (ConditionSpec spec : List.of(new FieldMatches("/a", junk), new FieldEquals(junk, "x"))) {
        try {
          Condition.compile(spec);
        } catch (IllegalArgumentException expected) {
          // invalid pointer or regex
        }
      }
    }
  }

  @Test
  void fieldEqualsAndPrefixAgreeWithTheirDefinition() {
    Random random = new Random(316);
    for (int i = 0; i < ROUNDS; i++) {
      String value = GeneratedInputs.text(random, 12, false);
      String prefix = value.substring(0, random.nextInt(value.length() + 1));
      JsonNode doc = JSON.createObjectNode().put("a", value);
      assertThat(Condition.compile(new FieldEquals("/a", value)).test(doc, "t", HeaderLookup.NONE))
          .isTrue();
      assertThat(Condition.compile(new FieldPrefix("/a", prefix)).test(doc, "t", HeaderLookup.NONE))
          .isTrue();
      assertThat(
              Condition.compile(new FieldPrefix("/a", value + "x"))
                  .test(doc, "t", HeaderLookup.NONE))
          .isFalse();
    }
  }

  private static JsonNode document(Random random) {
    var node = JSON.createObjectNode();
    for (String key : List.of("a", "b")) {
      switch (random.nextInt(7)) {
        case 0 -> node.put(key, GeneratedInputs.text(random, 12, false));
        case 1 -> node.put(key, random.nextLong());
        case 2 -> node.put(key, random.nextDouble());
        case 3 -> node.put(key, random.nextBoolean());
        case 4 -> node.putNull(key);
        case 5 -> node.putObject(key).put("x", 1);
        default -> node.putArray(key).add(1);
      }
    }
    if (random.nextBoolean()) {
      var c = node.putObject("c");
      if (random.nextBoolean()) {
        c.put("d", GeneratedInputs.text(random, 6, false));
      }
    } else if (random.nextBoolean()) {
      node.put("c", "scalar");
    }
    return node;
  }
}
