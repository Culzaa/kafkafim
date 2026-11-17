package io.github.matabar.kafkafim.decode;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One configured transformation step in the shape {@code decode} compiles: the {@code op} and every
 * field a step can carry (DESIGN §8, ADR-0047). {@code decode} may not depend on {@code config}, so
 * the supervisor maps each bound step onto this record; a field an op does not take is {@code
 * null}.
 *
 * @param op the operation name, e.g. {@code remove}
 * @param field JSON pointer to the member the step reads or writes
 * @param to JSON pointer to the target of a move
 * @param value a JSON string to write
 * @param json the text of one JSON value to write
 */
public record TransformStepSpec(
    String op,
    @Nullable String field,
    @Nullable String to,
    @Nullable String value,
    @Nullable String json) {

  /** Requires the op. */
  public TransformStepSpec {
    Objects.requireNonNull(op, "op");
  }
}
