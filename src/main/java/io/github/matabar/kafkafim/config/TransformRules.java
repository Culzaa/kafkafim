package io.github.matabar.kafkafim.config;

import io.github.matabar.kafkafim.config.Pipeline.Step;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.JacksonException;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.core.json.JsonFactory;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Validation of a route's {@code transform} steps (DESIGN §8, ADR-0047): the checks every step
 * shares, and one {@link Op} entry per op for what the op takes.
 *
 * <p>Shared checks: at most {@value #MAX_STEPS} steps; a known {@code op}; no field the op does not
 * take and every field it requires; {@code field} and {@code to} are non-empty JSON pointers;
 * {@code value} and {@code json} fit {@value #MAX_TEXT_BYTES} bytes; {@code json} is exactly one
 * JSON value without a duplicate key. An op adds an {@link Op} for its field list and its own
 * cross-field checks, and nothing else here changes. {@code config} may not depend on {@code
 * decode}, which compiles the same ops under its own registry; a test keeps the two op lists equal.
 */
public final class TransformRules {

  /** Most steps in one route's {@code transform}. */
  static final int MAX_STEPS = 32;

  /** Longest {@code value} or {@code json} text, in UTF-8 bytes. */
  static final int MAX_TEXT_BYTES = 4096;

  private static final List<String> FIELDS = List.of("field", "to", "value", "json");

  private static final JsonMapper JSON =
      JsonMapper.builder(
              JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build())
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
          .build();

  /** What one op takes, and its own checks. */
  record Op(Set<String> required, Set<String> optional, Check check) {

    /** An op whose fields are all it needs checked. */
    static Op of(Set<String> required, Set<String> optional) {
      return new Op(required, optional, (step, problem) -> {});
    }
  }

  /** Cross-field checks of one op, for a step whose fields already passed the shared checks. */
  @FunctionalInterface
  interface Check {

    /** Reports each problem as a message about the step. */
    void check(Step step, Consumer<String> problem);
  }

  /** The production ops: one entry each, added with the op (ADR-0047). */
  static final Map<String, Op> BUILT_IN =
      Map.of(
          "remove",
          Op.of(Set.of("field"), Set.of()),
          "rename",
          new Op(Set.of("field", "to"), Set.of(), TransformRules::checkRename));

  private TransformRules() {}

  /** {@code rename} must move a member somewhere else: not onto itself, not into or out of it. */
  private static void checkRename(Step step, Consumer<String> problem) {
    String from = step.field();
    String to = step.to();
    if (from.equals(to)) {
      problem.accept("op 'rename' needs 'field' and 'to' to differ");
    } else if (to.startsWith(from + "/") || from.startsWith(to + "/")) {
      problem.accept("op 'rename' needs neither of 'field' and 'to' to be a prefix of the other");
    }
  }

  /** The names of the ops {@code transform} accepts. */
  public static Set<String> ops() {
    return new TreeSet<>(BUILT_IN.keySet());
  }

  /**
   * Checks one route's steps.
   *
   * @param path the property path of the route's {@code transform}
   * @param steps the steps, in order
   * @param ops the ops that are known
   * @param error receives {@code (path, message)} for every problem
   */
  static void check(
      String path, List<Step> steps, Map<String, Op> ops, BiConsumer<String, String> error) {
    if (steps.size() > MAX_STEPS) {
      error.accept(path, "at most " + MAX_STEPS + " steps per route, found " + steps.size());
    }
    for (int i = 0; i < steps.size(); i++) {
      String stepPath = path + "[" + i + "]";
      Step step = steps.get(i);
      if (step == null) {
        error.accept(stepPath, "must not be empty");
        continue;
      }
      checkStep(stepPath, step, ops, error);
    }
  }

  private static void checkStep(
      String path, Step step, Map<String, Op> ops, BiConsumer<String, String> error) {
    String name = step.op();
    if (name == null || name.isBlank()) {
      error.accept(path + ".op", "is required; known ops: " + known(ops));
      return;
    }
    Op op = ops.get(name);
    if (op == null) {
      error.accept(path + ".op", "unknown op '" + name + "'; known ops: " + known(ops));
      return;
    }
    boolean fieldsOk = true;
    for (String field : FIELDS) {
      boolean present = text(step, field) != null;
      boolean takes = op.required().contains(field) || op.optional().contains(field);
      if (present && !takes) {
        error.accept(path + "." + field, "op '" + name + "' takes no '" + field + "'");
        fieldsOk = false;
      } else if (!present && op.required().contains(field)) {
        error.accept(path + "." + field, "op '" + name + "' needs '" + field + "'");
        fieldsOk = false;
      }
    }
    fieldsOk &= checkPointer(path + ".field", step.field(), error);
    fieldsOk &= checkPointer(path + ".to", step.to(), error);
    fieldsOk &= checkText(path + ".value", step.value(), error);
    fieldsOk &= checkJson(path + ".json", step.json(), error);
    if (fieldsOk) {
      op.check().check(step, message -> error.accept(path, message));
    }
  }

  private static String known(Map<String, Op> ops) {
    return ops.isEmpty() ? "none" : String.join(", ", new TreeSet<>(ops.keySet()));
  }

  private static @Nullable String text(Step step, String field) {
    return switch (field) {
      case "field" -> step.field();
      case "to" -> step.to();
      case "value" -> step.value();
      default -> step.json();
    };
  }

  private static boolean checkPointer(
      String path, @Nullable String pointer, BiConsumer<String, String> error) {
    if (pointer == null) {
      return true;
    }
    String problem = JsonPointers.error(pointer);
    if (problem != null) {
      error.accept(path, problem);
    }
    return problem == null;
  }

  private static boolean checkText(
      String path, @Nullable String text, BiConsumer<String, String> error) {
    if (text == null) {
      return true;
    }
    int bytes = text.getBytes(StandardCharsets.UTF_8).length;
    if (bytes > MAX_TEXT_BYTES) {
      error.accept(path, "is " + bytes + " bytes, at most " + MAX_TEXT_BYTES + " are allowed");
      return false;
    }
    return true;
  }

  private static boolean checkJson(
      String path, @Nullable String json, BiConsumer<String, String> error) {
    if (!checkText(path, json, error) || json == null) {
      return json == null;
    }
    try {
      JsonNode node = JSON.readTree(json);
      if (node.isMissingNode()) {
        error.accept(path, "is empty; it must be one JSON value");
        return false;
      }
      return true;
    } catch (JacksonException e) {
      // The text is not echoed: it may carry a value the operator keeps out of logs.
      error.accept(
          path, "is not exactly one JSON value (invalid syntax, trailing text or a duplicate key)");
      return false;
    }
  }
}
