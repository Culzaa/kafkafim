package io.github.culzaa.kafkafim.decode;

/**
 * Why a record or a document could not be decoded. {@link #wireName()} is the DLQ {@code
 * kafkafim.error.type} header value and the {@code error_type} metric label (DESIGN §7, §9).
 */
public enum ErrorType {
  /**
   * The value is not a JSON object or array of objects, an array element is not an object, the
   * value is larger than {@code max-record-bytes}, or a configured version is unusable.
   */
  DECODE("decode"),
  /** The {@code _id} pointer resolves to nothing usable and {@code id-missing} is {@code dlq}. */
  MISSING_ID("missing_id"),
  /** The timestamp is missing or unparsable and {@code on-bad-timestamp} is {@code dlq}. */
  BAD_TIMESTAMP("bad_timestamp"),
  /**
   * A route's {@code transform} step cannot apply to the document, e.g. a parent that is not an
   * object (ADR-0047). Raised for one routed copy, never by the decoder itself.
   */
  TRANSFORM("transform");

  private final String wireName;

  ErrorType(String wireName) {
    this.wireName = wireName;
  }

  /** The {@code kafkafim.error.type} header value. */
  public String wireName() {
    return wireName;
  }
}
