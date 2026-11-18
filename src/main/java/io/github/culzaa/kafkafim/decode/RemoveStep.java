package io.github.culzaa.kafkafim.decode;

import tools.jackson.databind.JsonNode;
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

  RemoveStep(String pointer) {
    this.pointer = pointer;
    this.keys = Pointers.keys("remove", pointer);
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
      ObjectNode copy = Pointers.shallowCopy(node);
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
    ObjectNode copy = Pointers.shallowCopy(node);
    copy.set(key, changed); // replacing keeps the member's position
    return copy;
  }
}
