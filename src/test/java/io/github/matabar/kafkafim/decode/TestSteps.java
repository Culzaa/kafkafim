package io.github.matabar.kafkafim.decode;

import java.util.Map;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * Test-only ops for the transform framework, which has no production op of its own: {@code noop},
 * {@code append} (adds {@code <field>: <value>} at the end of the root), {@code fail} (always
 * fails) and {@code wrap} (nests the root {@code value} levels deeper).
 */
public final class TestSteps {

  private static final JsonMapper MAPPER = JsonMapper.builder().build();

  /** Returns its input untouched. */
  public static final StepRegistry.Factory NOOP =
      spec ->
          new TransformStep() {
            @Override
            public JsonNode apply(JsonNode root) {
              return root;
            }

            @Override
            public String describe() {
              return "noop";
            }
          };

  /** Appends {@code <field>: <value>} to the root, copy on write. */
  public static final StepRegistry.Factory APPEND =
      spec ->
          new TransformStep() {
            @Override
            public JsonNode apply(JsonNode root) {
              ObjectNode copy = ((ObjectNode) root).deepCopy();
              copy.put(spec.field(), spec.value());
              return copy;
            }

            @Override
            public String describe() {
              return "append " + spec.field();
            }
          };

  /** Always fails. */
  public static final StepRegistry.Factory FAIL =
      spec ->
          new TransformStep() {
            @Override
            public JsonNode apply(JsonNode root) throws TransformException {
              throw new TransformException("parent is not an object");
            }

            @Override
            public String describe() {
              return "fail " + spec.field();
            }
          };

  /** Wraps the root in {@code spec.value()} levels of objects. */
  public static final StepRegistry.Factory WRAP =
      spec ->
          new TransformStep() {
            @Override
            public JsonNode apply(JsonNode root) {
              JsonNode current = root;
              for (int i = 0; i < Integer.parseInt(spec.value()); i++) {
                current = MAPPER.createObjectNode().set("a", current);
              }
              return current;
            }

            @Override
            public String describe() {
              return "wrap";
            }

            @Override
            public boolean deepens() {
              return true;
            }
          };

  public static final StepRegistry REGISTRY =
      StepRegistry.of(Map.of("noop", NOOP, "append", APPEND, "fail", FAIL, "wrap", WRAP));

  private TestSteps() {}
}
