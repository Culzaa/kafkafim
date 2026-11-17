package io.github.matabar.kafkafim.config;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.matabar.kafkafim.config.Pipeline.Step;
import io.github.matabar.kafkafim.config.TransformRules.Op;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** The shared checks of {@code transform} steps, with test ops: the real ops add their own. */
class TransformRulesTest {

  /** Takes {@code field}, optionally {@code to}; rejects {@code field: /bad} in its own check. */
  private static final Map<String, Op> OPS =
      Map.of(
          "noop",
          Op.of(Set.of(), Set.of("field")),
          "move",
          new Op(
              Set.of("field", "to"),
              Set.of(),
              (step, problem) -> {
                if (step.field().equals(step.to())) {
                  problem.accept("field and to are equal");
                }
              }),
          "write",
          Op.of(Set.of("field"), Set.of("value", "json")));

  private static Step step(String op, String field, String to, String value, String json) {
    return new Step(op, field, to, value, json);
  }

  private static List<String> check(Step... steps) {
    List<String> errors = new ArrayList<>();
    TransformRules.check(
        "p.routes[0].transform",
        Arrays.asList(steps),
        OPS,
        (path, message) -> errors.add(path + ": " + message));
    return errors;
  }

  @Test
  void acceptsValidSteps() {
    assertThat(check(step("noop", null, null, null, null), step("move", "/a", "/b", null, null)))
        .isEmpty();
  }

  @Test
  void rejectsAnUnknownOrMissingOpAndListsTheKnownOnes() {
    assertThat(check(step("bogus", null, null, null, null), step(null, "/a", null, null, null)))
        .containsExactly(
            "p.routes[0].transform[0].op: unknown op 'bogus'; known ops: move, noop, write",
            "p.routes[0].transform[1].op: is required; known ops: move, noop, write");
  }

  @Test
  void rejectsFieldsTheOpDoesNotTakeAndMissingRequiredOnes() {
    assertThat(check(step("noop", null, "/b", null, null), step("move", "/a", null, null, null)))
        .containsExactly(
            "p.routes[0].transform[0].to: op 'noop' takes no 'to'",
            "p.routes[0].transform[1].to: op 'move' needs 'to'");
  }

  @Test
  void rejectsBadPointers() {
    assertThat(check(step("move", "a", "", null, null), step("write", "/a~2", null, null, null)))
        .containsExactly(
            "p.routes[0].transform[0].field: 'a' is not a JSON pointer (must start with '/')",
            "p.routes[0].transform[0].to: JSON pointer is empty",
            "p.routes[0].transform[1].field: '/a~2' is not a valid JSON pointer: '~' must be"
                + " followed by '0' or '1'");
  }

  @Test
  void limitsTheTextOfValueAndJson() {
    String ok = "x".repeat(4096);
    String big = "x".repeat(4097);
    assertThat(check(step("write", "/a", null, ok, null))).isEmpty();
    assertThat(check(step("write", "/a", null, big, null)))
        .containsExactly("p.routes[0].transform[0].value: is 4097 bytes, at most 4096 are allowed");
    assertThat(check(step("write", "/a", null, null, "\"" + "é".repeat(2049) + "\"")))
        .singleElement()
        .asString()
        .contains(".json: is 4100 bytes");
  }

  @Test
  void jsonMustBeExactlyOneValueWithoutDuplicateKeys() {
    for (String valid : List.of("2", "true", "null", "\"s\"", "{\"a\":[1,{\"b\":2}]}", "[1, 2]")) {
      assertThat(check(step("write", "/a", null, null, valid))).as(valid).isEmpty();
    }
    for (String invalid :
        List.of("", "{", "1 2", "{\"a\":1,\"a\":2}", "{\"x\":{\"a\":1,\"a\":2}}")) {
      assertThat(check(step("write", "/a", null, null, invalid)))
          .as(invalid)
          .singleElement()
          .asString()
          .startsWith("p.routes[0].transform[0].json: ");
    }
  }

  private static String nested(int levels) {
    return "[".repeat(levels) + "]".repeat(levels);
  }

  @Test
  void jsonNestingMustFitUnderTheLimitAtItsTarget() {
    // 499 levels under a one-token field is 500 deep: allowed.
    assertThat(check(step("write", "/a", null, null, nested(499)))).isEmpty();
    // 500 levels fits only at the root, which a field never is.
    assertThat(check(step("write", "/a", null, null, nested(500))))
        .singleElement()
        .asString()
        .startsWith("p.routes[0].transform[0].json: nests 500 deep")
        .contains("deeper than 500");
    // A deeper pointer leaves less room.
    assertThat(check(step("write", "/a/b/c", null, null, nested(497)))).isEmpty();
    assertThat(check(step("write", "/a/b/c", null, null, nested(498)))).hasSize(1);
    // Beyond the parser's own limit.
    assertThat(check(step("write", "/a", null, null, nested(501))))
        .singleElement()
        .asString()
        .isEqualTo("p.routes[0].transform[0].json: nests deeper than 500");
    // A scalar adds no level; escaped slashes are one token.
    assertThat(check(step("write", "/a~1b", null, null, nested(499)))).isEmpty();
  }

  @Test
  void jsonErrorsDoNotEchoTheText() {
    assertThat(check(step("write", "/a", null, null, "{\"secret\": nope}")).toString())
        .doesNotContain("secret", "nope");
  }

  @Test
  void runsOpOwnCheckOnlyOnceSharedChecksPass() {
    assertThat(check(step("move", "/a", "/a", null, null)))
        .containsExactly("p.routes[0].transform[0]: field and to are equal");
    assertThat(check(step("move", "/a", "b", null, null)))
        .containsExactly(
            "p.routes[0].transform[0].to: 'b' is not a JSON pointer (must start with '/')");
  }

  @Test
  void allowsAtMostThirtyTwoSteps() {
    Step noop = step("noop", null, null, null, null);
    assertThat(check(java.util.Collections.nCopies(32, noop).toArray(Step[]::new))).isEmpty();
    assertThat(check(java.util.Collections.nCopies(33, noop).toArray(Step[]::new)))
        .containsExactly("p.routes[0].transform: at most 32 steps per route, found 33");
  }
}
