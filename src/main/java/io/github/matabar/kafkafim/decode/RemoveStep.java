package io.github.matabar.kafkafim.decode;

import java.util.Map;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;

/**
 * The {@code remove} op (DESIGN §8, ADR-0047): deletes the object member a JSON pointer names. A
 * missing member, or a parent on the way that is absent or not an object, is a no-op; a {@code
 * null} member is present and is deleted. Pointer tokens are always object keys.
 */
final class RemoveStep implements TransformStep {

  /** The registry entry of the op. */
  static final StepRegistry.Factory FACTORY = spec -> new RemoveStep(spec.field());

  private final String pointer;
  private final String[] keys;

  private RemoveStep(String pointer) {
    if (pointer == null || !pointer.startsWith("/")) {
      throw new IllegalArgumentException("remove needs a non-empty JSON pointer in 'field'");
    }
    this.pointer = pointer;
    String[] tokens = pointer.substring(1).split("/", -1);
    for (int i = 0; i < tokens.length; i++) {
      tokens[i] = tokens[i].replace("~1", "/").replace("~0", "~");
    }
    this.keys = tokens;
  }

  @Override
  public JsonNode apply(JsonNode root) {
    return root instanceof ObjectNode object ? remove(object, 0) : root;
  }

  @Override
  public String describe() {
    return "remove " + pointer;
  }

  /** {@code node} itself when nothing changed, else a copy that shares every untouched node. */
  private JsonNode remove(ObjectNode node, int depth) {
    String key = keys[depth];
    JsonNode child = node.get(key);
    if (child == null) {
      return node;
    }
    if (depth == keys.length - 1) {
      ObjectNode copy = shallowCopy(node);
      copy.remove(key);
      return copy;
    }
    if (!(child instanceof ObjectNode childObject)) {
      return node;
    }
    JsonNode changed = remove(childObject, depth + 1);
    if (changed == child) {
      return node;
    }
    ObjectNode copy = shallowCopy(node);
    copy.set(key, changed); // replacing keeps the member's position
    return copy;
  }

  private static ObjectNode shallowCopy(ObjectNode node) {
    ObjectNode copy = JsonNodeFactory.instance.objectNode();
    for (Map.Entry<String, JsonNode> member : node.properties()) {
      copy.set(member.getKey(), member.getValue());
    }
    return copy;
  }
}
