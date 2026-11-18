package io.github.culzaa.kafkafim.decode;

import java.util.Map;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;

/** JSON pointer parsing shared by the ops (DESIGN §8, ADR-0047): every token is an object key. */
final class Pointers {

  private Pointers() {}

  /**
   * The object keys of a non-empty pointer, unescaped ({@code ~1} is {@code /}, {@code ~0} is
   * {@code ~}).
   *
   * @throws IllegalArgumentException if {@code pointer} is {@code null} or does not start with
   *     {@code /} (a bound step has already passed validation, so this is a defensive check)
   */
  static String[] keys(String op, String pointer) {
    if (pointer == null || !pointer.startsWith("/")) {
      throw new IllegalArgumentException(op + " needs a non-empty JSON pointer");
    }
    String[] tokens = pointer.substring(1).split("/", -1);
    for (int i = 0; i < tokens.length; i++) {
      tokens[i] = tokens[i].replace("~1", "/").replace("~0", "~");
    }
    return tokens;
  }

  /** A new object with the same members in the same order; the member nodes are shared. */
  static ObjectNode shallowCopy(ObjectNode node) {
    ObjectNode copy = JsonNodeFactory.instance.objectNode();
    for (Map.Entry<String, JsonNode> member : node.properties()) {
      copy.set(member.getKey(), member.getValue());
    }
    return copy;
  }

  /**
   * A copy of {@code node} with {@code value} written at {@code keys[depth..]}; never mutates
   * {@code node}. An existing member is replaced in place, a new one goes last, and missing parent
   * objects are created.
   *
   * @throws TransformException with {@code parentMessage} if a parent exists but is not an object
   */
  static ObjectNode write(
      ObjectNode node, String[] keys, int depth, JsonNode value, String parentMessage)
      throws TransformException {
    String key = keys[depth];
    ObjectNode copy = shallowCopy(node);
    if (depth == keys.length - 1) {
      copy.set(key, value);
      return copy;
    }
    JsonNode child = node.get(key);
    ObjectNode parent;
    if (child == null) {
      parent = JsonNodeFactory.instance.objectNode();
    } else if (child instanceof ObjectNode childObject) {
      parent = childObject;
    } else {
      throw new TransformException(parentMessage);
    }
    copy.set(key, write(parent, keys, depth + 1, value, parentMessage));
    return copy;
  }
}
