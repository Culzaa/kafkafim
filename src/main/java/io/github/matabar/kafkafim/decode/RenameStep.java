package io.github.matabar.kafkafim.decode;

import java.util.Map;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.JsonNodeFactory;
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
    return write((ObjectNode) removed, 0, value);
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

  /**
   * A copy of {@code node} with {@code value} written at the target; never mutates {@code node}.
   */
  private ObjectNode write(ObjectNode node, int depth, JsonNode value) throws TransformException {
    String key = toKeys[depth];
    ObjectNode copy = shallowCopy(node);
    if (depth == toKeys.length - 1) {
      copy.set(key, value); // replacing keeps the position, a new key goes last
      return copy;
    }
    JsonNode child = node.get(key);
    ObjectNode parent;
    if (child == null) {
      parent = JsonNodeFactory.instance.objectNode();
    } else if (child instanceof ObjectNode childObject) {
      parent = childObject;
    } else {
      throw new TransformException("a parent of the target is not an object");
    }
    copy.set(key, write(parent, depth + 1, value));
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
