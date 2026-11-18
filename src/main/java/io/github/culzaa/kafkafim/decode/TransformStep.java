package io.github.culzaa.kafkafim.decode;

import tools.jackson.databind.JsonNode;

/**
 * One compiled, immutable transformation step of a route (DESIGN §8, ADR-0047), built once when the
 * pipeline starts and shared by every consumer thread.
 *
 * <p>Steps are copy-on-write: {@link #apply} never mutates its input, which may be shared by the
 * other routes' copies of the same document. It returns a root that copies only the object nodes
 * along the changed paths and shares every other node.
 */
public interface TransformStep {

  /**
   * Applies the step.
   *
   * @param root the document's root object (read-only)
   * @return the root after the step: {@code root} itself for a step that changed nothing
   * @throws TransformException if the step cannot apply to this document; the message names no
   *     value
   */
  JsonNode apply(JsonNode root) throws TransformException;

  /** The op and its pointers, for failure reasons, e.g. {@code rename /usr to /user}. */
  String describe();

  /**
   * Whether the step can make the document nest deeper; if so the framework checks the depth after
   * it runs and fails the copy beyond {@value Decoder#MAX_NESTING_DEPTH}.
   */
  default boolean deepens() {
    return false;
  }
}
