package io.github.matabar.kafkafim.route;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.JsonPointer;
import tools.jackson.databind.JsonNode;

/** A compiled {@link ConditionSpec}: all parsing done, evaluation never throws. */
@FunctionalInterface
interface Condition {

  /**
   * Whether the condition holds.
   *
   * @param onBudgetExceeded called when a {@code matches} regex ran past its step budget (the
   *     condition then does not hold); see {@link RegexBudget}
   */
  boolean test(JsonNode document, String topic, HeaderLookup headers, Runnable onBudgetExceeded);

  /** As {@link #test(JsonNode, String, HeaderLookup, Runnable)}, ignoring a budget overrun. */
  default boolean test(JsonNode document, String topic, HeaderLookup headers) {
    return test(document, topic, headers, () -> {});
  }

  /**
   * Compiles a spec.
   *
   * @throws IllegalArgumentException for an invalid pointer or regex
   */
  static Condition compile(@Nullable ConditionSpec spec) {
    Specs.require(spec != null, "condition must not be null");
    return switch (spec) {
      case ConditionSpec.FieldEquals(String field, String value) -> {
        JsonPointer pointer = Specs.pointer(field);
        ScalarValue expected = ScalarValue.of(value);
        yield (document, topic, headers, overrun) -> expected.matches(document.at(pointer));
      }
      case ConditionSpec.FieldIn(String field, List<String> values) -> {
        JsonPointer pointer = Specs.pointer(field);
        ScalarValue[] expected = values.stream().map(ScalarValue::of).toArray(ScalarValue[]::new);
        yield (document, topic, headers, overrun) -> {
          JsonNode node = document.at(pointer);
          for (ScalarValue value : expected) {
            if (value.matches(node)) {
              return true;
            }
          }
          return false;
        };
      }
      case ConditionSpec.FieldExists(String field, boolean exists) -> {
        JsonPointer pointer = Specs.pointer(field);
        yield (document, topic, headers, overrun) -> {
          JsonNode node = document.at(pointer);
          return (!node.isMissingNode() && !node.isNull()) == exists;
        };
      }
      case ConditionSpec.FieldPrefix(String field, String prefix) -> {
        JsonPointer pointer = Specs.pointer(field);
        yield (document, topic, headers, overrun) -> {
          JsonNode node = document.at(pointer);
          return node.isString() && node.stringValue().startsWith(prefix);
        };
      }
      case ConditionSpec.FieldMatches(String field, String regex) -> {
        JsonPointer pointer = Specs.pointer(field);
        Pattern pattern = Pattern.compile(regex);
        yield (document, topic, headers, overrun) -> {
          JsonNode node = document.at(pointer);
          return node.isString() && matches(pattern, node.stringValue(), overrun);
        };
      }
      case ConditionSpec.HeaderEquals(String name, String value) -> {
        byte[] expected = value.getBytes(StandardCharsets.UTF_8);
        yield (document, topic, headers, overrun) -> {
          byte[] actual = headers.lastValue(name);
          return actual != null && Arrays.equals(actual, expected);
        };
      }
      case ConditionSpec.TopicEquals(String expected) ->
          (document, topic, headers, overrun) -> expected.equals(topic);
    };
  }

  /**
   * Anchored match. {@code java.util.regex} backtracks, so a pattern such as {@code (.*a){12}} takes
   * exponential time on a value like {@code aaaa...!}, and recurses per repetition of some
   * constructs (e.g. {@code (a|b)*}), so a long enough value overflows the stack. Both count as no
   * match rather than stalling or killing the consumer thread on document content: the first by a
   * {@link RegexBudget} on the characters the matcher reads (reported to {@code onBudgetExceeded}),
   * the second by catching the {@link StackOverflowError}.
   */
  private static boolean matches(Pattern pattern, String value, Runnable onBudgetExceeded) {
    try {
      return pattern.matcher(new RegexBudget(value)).matches();
    } catch (RegexBudget.Exceeded e) {
      onBudgetExceeded.run();
      return false;
    } catch (StackOverflowError e) {
      return false;
    }
  }
}
