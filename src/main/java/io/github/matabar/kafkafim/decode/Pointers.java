package io.github.matabar.kafkafim.decode;

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
}
