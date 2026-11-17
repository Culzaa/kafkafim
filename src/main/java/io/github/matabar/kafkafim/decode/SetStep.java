package io.github.matabar.kafkafim.decode;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;

/**
 * The {@code set} op (DESIGN §8, ADR-0047): writes a constant at the object member a JSON pointer
 * names. The value is a JSON string ({@code value}) or any one JSON value ({@code json}, parsed
 * once when the step is compiled, with exact decimals). An existing member is overwritten in place,
 * a new one goes last, and missing parent objects are created; a parent that exists but is not an
 * object (a {@code null} included) is a {@link TransformException}. Pointer tokens are always
 * object keys.
 */
final class SetStep implements TransformStep {

  /** The registry entry of the op. */
  static final StepRegistry.Factory FACTORY = spec -> new SetStep(spec);

  private final String pointer;
  private final String[] keys;
  private final JsonNode constant;

  private SetStep(TransformStepSpec spec) {
    this.pointer = spec.field();
    this.keys = Pointers.keys("set", pointer);
    if (spec.json() != null) {
      try {
        this.constant = DocumentTransform.EXACT.readTree(spec.json());
      } catch (JacksonException e) {
        throw new IllegalArgumentException("set needs 'json' to be one JSON value");
      }
    } else if (spec.value() != null) {
      this.constant = JsonNodeFactory.instance.stringNode(spec.value());
    } else {
      throw new IllegalArgumentException("set needs one of 'value' and 'json'");
    }
  }

  @Override
  public JsonNode apply(JsonNode root) throws TransformException {
    if (!(root instanceof ObjectNode object)) {
      return root;
    }
    return Pointers.write(object, keys, 0, constant, "a parent of the field is not an object");
  }

  @Override
  public String describe() {
    return "set " + pointer;
  }

  /** Creating the parents, or a nested constant, can nest the document deeper. */
  @Override
  public boolean deepens() {
    return true;
  }
}
