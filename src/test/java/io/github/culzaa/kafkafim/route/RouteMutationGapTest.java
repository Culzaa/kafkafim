package io.github.culzaa.kafkafim.route;

import static io.github.culzaa.kafkafim.route.ConditionTest.doc;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.culzaa.kafkafim.route.ConditionSpec.FieldEquals;
import io.github.culzaa.kafkafim.route.ConditionSpec.FieldExists;
import io.github.culzaa.kafkafim.route.ConditionSpec.FieldIn;
import io.github.culzaa.kafkafim.route.ConditionSpec.FieldMatches;
import io.github.culzaa.kafkafim.route.ConditionSpec.FieldPrefix;
import io.github.culzaa.kafkafim.route.ConditionSpec.HeaderEquals;
import io.github.culzaa.kafkafim.route.ConditionSpec.TopicEquals;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests added after the first PIT run (docs/DELIVERY.md, "Mutation testing"): validation messages,
 * the exact limits of the index-name rules, and the scalar-to-text rules of index patterns.
 */
class RouteMutationGapTest {

  private static final char HIGH = (char) 0xD83D;
  private static final char LOW = (char) 0xDE00;
  private static final Instant EVENT_TIME = Instant.parse("2026-01-02T03:04:05Z");

  // ---- IndexNames -------------------------------------------------------------------------

  @Test
  void indexNameRulesNameTheRuleThatIsBroken() {
    assertThat(IndexNames.problem("")).isEqualTo("must not be empty");
    assertThat(IndexNames.problem(".")).isEqualTo("must not be '.' or '..'");
    assertThat(IndexNames.problem("..")).isEqualTo("must not be '.' or '..'");
    assertThat(IndexNames.problem("-a")).isEqualTo("must not start with '-'");
    assertThat(IndexNames.problem("_a")).isEqualTo("must not start with '_'");
    assertThat(IndexNames.problem("+a")).isEqualTo("must not start with '+'");
    assertThat(IndexNames.problem("a#b")).isEqualTo("must not contain '#'");
    assertThat(IndexNames.problem("a b")).isEqualTo("must not contain ' '");
    assertThat(IndexNames.problem("a\\b")).isEqualTo("must not contain '\\'");
    assertThat(IndexNames.problem("ok.name-1_2+3")).isNull();
    assertThat(IndexNames.problem(".hidden")).isNull();
  }

  @Test
  void indexNameLimitCountsUtf8BytesPerCharacterClass() {
    // One-byte characters: 255 pass, 256 fail.
    assertThat(IndexNames.problem("a".repeat(255))).isNull();
    assertThat(IndexNames.problem("a".repeat(256))).isEqualTo("is 256 UTF-8 bytes, more than 255");
    // U+007F is the last one-byte character, U+0080 the first two-byte one.
    assertThat(IndexNames.problem("\u007f".repeat(255))).isNull();
    assertThat(IndexNames.problem("\u0080".repeat(127) + "a")).isNull(); // 254 + 1 = 255
    assertThat(IndexNames.problem("\u0080".repeat(128)))
        .isEqualTo("is 256 UTF-8 bytes, more than 255");
    // U+07FF is the last two-byte character, U+0800 the first three-byte one.
    assertThat(IndexNames.problem("߿".repeat(127) + "a")).isNull();
    assertThat(IndexNames.problem("ࠀ".repeat(85))).isNull(); // 255
    assertThat(IndexNames.problem("ࠀ".repeat(85) + "a"))
        .isEqualTo("is 256 UTF-8 bytes, more than 255");
    // A surrogate pair is four bytes, not two times three.
    assertThat(IndexNames.problem("😀".repeat(63) + "abc")).isNull(); // 252 + 3
    assertThat(IndexNames.problem("😀".repeat(63) + "abcd"))
        .isEqualTo("is 256 UTF-8 bytes, more than 255");
  }

  @Test
  void unpairedSurrogatesAreRejectedAnywhere() {
    String message = "must not contain an unpaired UTF-16 surrogate";
    assertThat(IndexNames.problem("a" + HIGH)).isEqualTo(message);
    assertThat(IndexNames.problem("a" + HIGH + "b")).isEqualTo(message);
    assertThat(IndexNames.problem("a" + LOW)).isEqualTo(message);
    assertThat(IndexNames.problem("a" + HIGH + HIGH)).isEqualTo(message);
  }

  // ---- Specs and the spec records ---------------------------------------------------------

  @Test
  void pointerAcceptsRootAndEscapesAndRejectsTheRest() {
    assertThat(Specs.pointer("/").toString()).isEqualTo("/");
    assertThat(Specs.pointer("/a~0b/c~1d").toString()).isEqualTo("/a~0b/c~1d");
    assertThatThrownBy(() -> Specs.pointer("a")).hasMessage("'a' is not a JSON pointer");
    assertThatThrownBy(() -> Specs.pointer(null)).hasMessage("'null' is not a JSON pointer");
    assertThatThrownBy(() -> Specs.pointer("/a~"))
        .hasMessage("'/a~' is not a valid JSON pointer: '~' must be followed by '0' or '1'");
    assertThatThrownBy(() -> Specs.pointer("/a~2"))
        .hasMessage("'/a~2' is not a valid JSON pointer: '~' must be followed by '0' or '1'");
    assertThatThrownBy(() -> Specs.pointer("/a~0/b~")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void routeSpecRejectsEachInvalidCombination() {
    List<ConditionSpec> none = List.of();
    assertThatThrownBy(() -> new RouteSpec<>(null, none, false, "i"))
        .hasMessage("route target is required");
    assertThatThrownBy(() -> new RouteSpec<>("t", null, false, "i"))
        .hasMessage("when must be a list of conditions");
    assertThatThrownBy(() -> new RouteSpec<>("t", Arrays.asList((ConditionSpec) null), false, "i"))
        .hasMessage("when must be a list of conditions");
    assertThatThrownBy(() -> new RouteSpec<>("t", none, false, null))
        .hasMessage("route index is required");
    assertThatThrownBy(() -> new RouteSpec<>("t", none, false, "i", true))
        .hasMessage("a drop route takes no index");
    assertThatThrownBy(() -> new RouteSpec<>("t", List.of(new TopicEquals("x")), true, "i"))
        .hasMessage("the default route must not have conditions");
    assertThatCode(() -> RouteSpec.drop("t", none, true)).doesNotThrowAnyException();
    assertThatCode(() -> new RouteSpec<>("t", none, true, "i")).doesNotThrowAnyException();
  }

  @Test
  void routeSpecCopiesItsConditions() {
    List<ConditionSpec> mutable = new ArrayList<>(List.of(new TopicEquals("x")));
    RouteSpec<String> spec = new RouteSpec<>("t", mutable, false, "i");
    mutable.clear();
    assertThat(spec.when()).hasSize(1);
  }

  @Test
  void conditionSpecsRejectMissingComponents() {
    assertThatThrownBy(() -> new FieldEquals(null, "v")).hasMessage("field is required");
    assertThatThrownBy(() -> new FieldEquals("/a", null)).hasMessage("equals value is required");
    assertThatThrownBy(() -> new FieldIn(null, List.of("a"))).hasMessage("field is required");
    assertThatThrownBy(() -> new FieldIn("/a", null)).hasMessage("in requires at least one value");
    assertThatThrownBy(() -> new FieldIn("/a", List.of()))
        .hasMessage("in requires at least one value");
    assertThatThrownBy(() -> new FieldIn("/a", Arrays.asList("a", null)))
        .hasMessage("in values must not be null");
    assertThatThrownBy(() -> new FieldExists(null, true)).hasMessage("field is required");
    assertThatThrownBy(() -> new FieldPrefix(null, "p")).hasMessage("field is required");
    assertThatThrownBy(() -> new FieldPrefix("/a", null)).hasMessage("prefix is required");
    assertThatThrownBy(() -> new FieldMatches(null, "r")).hasMessage("field is required");
    assertThatThrownBy(() -> new FieldMatches("/a", null)).hasMessage("matches regex is required");
    assertThatThrownBy(() -> new HeaderEquals(null, "v")).hasMessage("header name is required");
    assertThatThrownBy(() -> new HeaderEquals(" ", "v")).hasMessage("header name is required");
    assertThatThrownBy(() -> new HeaderEquals("h", null))
        .hasMessage("header equals value is required");
    assertThatThrownBy(() -> new TopicEquals(null)).hasMessage("topic is required");
    assertThatThrownBy(() -> new TopicEquals(" ")).hasMessage("topic is required");
  }

  @Test
  void fieldInCopiesItsValues() {
    List<String> mutable = new ArrayList<>(List.of("a"));
    FieldIn in = new FieldIn("/a", mutable);
    mutable.add("b");
    assertThat(in.values()).containsExactly("a");
  }

  @Test
  void scalarValueRejectsNullAndPrintsItsText() {
    assertThatThrownBy(() -> ScalarValue.of(null)).hasMessage("value must not be null");
    assertThat(ScalarValue.of("1.50")).hasToString("1.50");
  }

  @Test
  void routerCompileRejectsMissingMode() {
    List<RouteSpec<String>> specs = List.of(new RouteSpec<>("t", List.of(), true, "i"));
    assertThatThrownBy(() -> Router.compile(null, specs)).hasMessage("route mode is required");
  }

  @Test
  void numbersAreComparedByValueNotByText() {
    assertThat(ConditionTest.test(new FieldEquals("/n", "1"), "{\"n\":2}")).isFalse();
    assertThat(ConditionTest.test(new FieldEquals("/n", "1.5"), "{\"n\":1.5}")).isTrue();
    assertThat(ConditionTest.test(new FieldEquals("/n", "1.5"), "{\"n\":1.6}")).isFalse();
    assertThat(ConditionTest.test(new FieldEquals("/n", "1"), "{\"n\":1.0}")).isTrue();
    assertThat(ConditionTest.test(new FieldEquals("/n", "abc"), "{\"n\":1}")).isFalse();
    assertThat(ConditionTest.test(new FieldEquals("/n", "9223372036854775808"), "{\"n\":-1}"))
        .isFalse();
  }

  // ---- IndexPattern -----------------------------------------------------------------------

  private static IndexResolution resolve(String pattern, String json) {
    return IndexPattern.compile(pattern).resolve(doc(json), EVENT_TIME);
  }

  @Test
  void patternExposesItsSourceTextAndTimeUse() {
    IndexPattern pattern = IndexPattern.compile("a-%{x}-%{+yyyy}");
    assertThat(pattern.pattern()).isEqualTo("a-%{x}-%{+yyyy}");
    assertThat(pattern).hasToString("a-%{x}-%{+yyyy}");
    assertThat(pattern.usesEventTime()).isTrue();
    assertThat(IndexPattern.compile("a-%{x}").usesEventTime()).isFalse();
  }

  @Test
  void patternResolvesScalarsToLowerCasedText() {
    assertThat(resolve("a-%{x}", "{\"x\":\"EU\"}")).isEqualTo(new IndexResolution.Resolved("a-eu"));
    assertThat(resolve("a-%{x}", "{\"x\":42}")).isEqualTo(new IndexResolution.Resolved("a-42"));
    assertThat(resolve("a-%{x}", "{\"x\":true}")).isEqualTo(new IndexResolution.Resolved("a-true"));
    assertThat(resolve("a-%{+yyyy.MM}-%{x}", "{\"x\":\"b\"}"))
        .isEqualTo(new IndexResolution.Resolved("a-2026.01-b"));
  }

  @Test
  void patternNamesWhyFieldIsUnusable() {
    assertThat(resolve("a-%{x}", "{}")).isEqualTo(new IndexResolution.Unresolved("x", "missing"));
    assertThat(resolve("a-%{x}", "{\"x\":null}"))
        .isEqualTo(new IndexResolution.Unresolved("x", "null"));
    assertThat(resolve("a-%{x}", "{\"x\":{}}"))
        .isEqualTo(new IndexResolution.Unresolved("x", "an object"));
    assertThat(resolve("a-%{x}", "{\"x\":[]}"))
        .isEqualTo(new IndexResolution.Unresolved("x", "an array"));
  }

  @Test
  void patternReportsAnInvalidResolvedName() {
    assertThat(resolve("a-%{x}", "{\"x\":\"b c\"}"))
        .isEqualTo(new IndexResolution.InvalidName("a-b c", "must not contain ' '"));
  }

  @Test
  void patternCompileNamesTheMalformedPart() {
    assertThatThrownBy(() -> IndexPattern.compile("a-%{x"))
        .hasMessage("unterminated '%{' at position 2 in 'a-%{x'");
    assertThatThrownBy(() -> IndexPattern.compile("a-%{}")).hasMessage("empty reference '%{}'");
    assertThatThrownBy(() -> IndexPattern.compile(" ")).hasMessage("index pattern is empty");
    assertThatThrownBy(() -> IndexPattern.compile(null)).hasMessage("index pattern is empty");
    assertThatThrownBy(() -> IndexPattern.compile("_a"))
        .hasMessage("resolved index name '_a' is invalid: must not start with '_'");
  }
}
