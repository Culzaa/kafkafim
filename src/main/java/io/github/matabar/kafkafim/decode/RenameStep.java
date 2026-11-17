package io.github.matabar.kafkafim.decode;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

/**
 * The {@code rename} op (DESIGN §8, ADR-0047): moves the object member a JSON pointer names to
 * another pointer. A missing source, or a source parent that is absent or not an object, is a
 * no-op; a {@code null} member is present and moves. The target overwrites an existing member in
 * place and gets any missing parent objects; a target parent that exists but is not an object is a
 * {@link TransformException}. The source is removed first, then the target is written by the usual
 * key-order rule (a replaced member keeps its position, a new one goes last). Pointer tokens are
 * always object keys.
 */
final class RenameStep implements TransformStep {

  /** The registry entry of the op. */
  static final StepRegistry.Factory FACTORY = spec -> new RenameStep(spec.field(), spec.to());

  private final String from;
  private final String to;
  private final String[] fromKeys;
  private final String[] toKeys;
  private final RemoveStep remove;

  private RenameStep(String from, String to) {
    this.fromKeys = Pointers.keys("rename", from);
    this.toKeys = Pointers.keys("rename", to);
    this.from = from;
    this.to = to;
    this.remove = new RemoveStep(from);
  }

  @Override
  public JsonNode apply(JsonNode root) throws TransformException {
    if (!(root instanceof ObjectNode object)) {
      return root;
    }
    JsonNode value = find(object);
    if (value == null) {
      return root;
    }
    JsonNode removed = remove.apply(root);
    return Pointers.write(
        (ObjectNode) removed, toKeys, 0, value, "a parent of the target is not an object");
  }

  @Override
  public String describe() {
    return "rename " + from + " to " + to;
  }

  /** Creating the target's parents can nest the document deeper. */
  @Override
  public boolean deepens() {
    return true;
  }

  private JsonNode find(ObjectNode root) {
    JsonNode node = root;
    for (String key : fromKeys) {
      if (!(node instanceof ObjectNode object)) {
        return null;
      }
      node = object.get(key);
      if (node == null) {
        return null;
      }
    }
    return node;
  }
}
