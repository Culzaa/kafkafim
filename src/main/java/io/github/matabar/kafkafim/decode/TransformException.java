package io.github.matabar.kafkafim.decode;

import java.io.Serial;

/**
 * A transformation step cannot apply to one document (a parent that exists but is not an object, or
 * a result nested too deep). The message names the step and never contains a document value
 * (ADR-0030, ADR-0047); the caller dead-letters the copy with {@code error.type=transform}.
 */
public final class TransformException extends Exception {

  @Serial private static final long serialVersionUID = 1L;

  /** Creates the exception; {@code message} must not contain a document value. */
  public TransformException(String message) {
    super(message, null, false, false);
  }
}
