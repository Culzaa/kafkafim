package io.github.culzaa.kafkafim.decode;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.JacksonException;
import tools.jackson.core.StreamReadConstraints;
import tools.jackson.core.json.JsonFactory;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.cfg.JsonNodeFeature;
import tools.jackson.databind.json.JsonMapper;

/**
 * One route's compiled {@code transform} steps (DESIGN §8, ADR-0047): immutable, built once per
 * route when the pipeline starts, shared by every consumer thread.
 *
 * <p>The decoder's tree is for routing and is not exact for decimals (it reads fractions and
 * exponents as {@code double}), so a transformed copy never serializes it. {@link #parseExact}
 * reads the document's original bytes again with exact decimals and big integers; the caller does
 * it once per document and passes the tree to {@link #apply} for each transformed route. The steps
 * are copy-on-write, so that tree is never mutated, and the result is serialized once as compact
 * one-line JSON (NDJSON-safe) into a {@link DocumentSource} that owns its bytes.
 *
 * <p>Numbers keep their exact value but not their spelling ({@code 1e3} is written {@code 1E+3},
 * {@code -0} is written {@code 0}); key order is kept, whitespace dropped, strings use Jackson's
 * minimal escapes.
 */
public final class DocumentTransform {

  /** The mapper for the second, exact parse and the serialization. */
  static final JsonMapper EXACT =
      JsonMapper.builder(
              JsonFactory.builder()
                  .streamReadConstraints(
                      StreamReadConstraints.builder()
                          .maxNestingDepth(Decoder.MAX_NESTING_DEPTH)
                          .maxNumberLength(Decoder.MAX_NUMBER_LENGTH)
                          .maxNameLength(Decoder.MAX_NAME_LENGTH)
                          .build())
                  .build())
          .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
          // 1.10 stays 1.10 and 0.0 stays 0.0: the exact value, written as parsed.
          .disable(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES)
          .build();

  private final String route;
  private final List<TransformStep> steps;

  private DocumentTransform(String route, List<TransformStep> steps) {
    this.route = route;
    this.steps = steps;
  }

  /**
   * Compiles {@code specs} with the production ops ({@link StepRegistry#BUILT_IN}).
   *
   * @param route the route's label, for failure reasons
   * @param specs the route's steps, in order
   * @return {@code null} when there are no steps: a route without {@code transform} keeps the
   *     zero-copy path
   * @throws IllegalArgumentException if a step names an unknown op or cannot be compiled
   */
  public static @Nullable DocumentTransform compile(String route, List<TransformStepSpec> specs) {
    return compile(route, specs, StepRegistry.BUILT_IN);
  }

  /** As {@link #compile(String, List)}, with the ops of {@code registry}. */
  public static @Nullable DocumentTransform compile(
      String route, List<TransformStepSpec> specs, StepRegistry registry) {
    Objects.requireNonNull(route, "route");
    Objects.requireNonNull(registry, "registry");
    if (specs.isEmpty()) {
      return null;
    }
    List<TransformStep> steps = new ArrayList<>(specs.size());
    for (int i = 0; i < specs.size(); i++) {
      TransformStepSpec spec = specs.get(i);
      StepRegistry.Factory factory = registry.factory(spec.op());
      if (factory == null) {
        throw new IllegalArgumentException(
            "route " + route + ", step " + (i + 1) + ": unknown op '" + spec.op() + "'");
      }
      steps.add(factory.compile(spec));
    }
    return new DocumentTransform(route, List.copyOf(steps));
  }

  /**
   * Parses a document's original bytes with exact decimals and big integers.
   *
   * @param original the document's source, already validated by the decoder
   * @return the root object; shared by the document's transformed copies, so never mutated
   * @throws TransformException if the bytes cannot be read again
   */
  public static JsonNode parseExact(DocumentSource original) throws TransformException {
    try {
      return EXACT.readTree(original.toByteArray());
    } catch (JacksonException | NumberFormatException e) {
      // NumberFormatException: a literal the routing parse reads as a double (1e99999999999)
      // but that has no exact BigDecimal.
      throw new TransformException("the document could not be parsed again for its transform");
    }
  }

  /**
   * Applies the steps in order, each on the result of the previous one, and serializes the result.
   *
   * @param exact the tree from {@link #parseExact}; not modified
   * @return a source that owns the transformed document's bytes
   * @throws TransformException if a step cannot apply; the message names the route, the step
   *     position, the op and its pointers, never a value
   */
  public DocumentSource apply(JsonNode exact) throws TransformException {
    JsonNode root = exact;
    for (int i = 0; i < steps.size(); i++) {
      TransformStep step = steps.get(i);
      try {
        root = step.apply(root);
      } catch (TransformException e) {
        throw new TransformException(prefix(i, step) + e.getMessage());
      }
      if (step.deepens() && deeperThan(root, Decoder.MAX_NESTING_DEPTH)) {
        throw new TransformException(
            prefix(i, step) + "the document would nest deeper than " + Decoder.MAX_NESTING_DEPTH);
      }
    }
    try {
      return DocumentSource.owning(EXACT.writeValueAsBytes(root));
    } catch (JacksonException e) {
      throw new TransformException(
          "route " + route + ": the transformed document could not be serialized");
    }
  }

  /** The number of steps. */
  public int size() {
    return steps.size();
  }

  private String prefix(int index, TransformStep step) {
    return "route " + route + ", step " + (index + 1) + " (" + step.describe() + "): ";
  }

  /** Whether objects and arrays nest deeper than {@code limit}, counting the root as level 1. */
  private static boolean deeperThan(JsonNode node, int limit) {
    return depthExceeds(node, 1, limit);
  }

  private static boolean depthExceeds(JsonNode node, int level, int limit) {
    if (!node.isContainer()) {
      return false;
    }
    if (level > limit) {
      return true;
    }
    for (JsonNode child : node.values()) {
      if (depthExceeds(child, level + 1, limit)) {
        return true;
      }
    }
    return false;
  }
}
