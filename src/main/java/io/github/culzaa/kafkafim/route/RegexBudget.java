package io.github.culzaa.kafkafim.route;

/**
 * A {@link CharSequence} that counts the characters a regex matcher reads and throws {@link
 * Exceeded} past a budget, which bounds a catastrophic-backtracking pattern ({@code (.*a){12}}) on
 * hostile document content. The budget is generous: {@value #BASE_STEPS} plus {@value
 * #STEPS_PER_CHAR} per character of the value, so linear and mildly quadratic patterns on any value
 * that fits a record never reach it. A hostile pattern is cut off after roughly 80 ms on a short
 * value and about 300 ms on a 100,000-character one (measured on JDK 27). One instance per
 * evaluation; not thread-safe.
 */
final class RegexBudget implements CharSequence {

  static final long BASE_STEPS = 10_000_000L;
  static final long STEPS_PER_CHAR = 1_000L;

  /** The budget ran out; carries no stack trace, as it is expected on hostile content. */
  static final class Exceeded extends RuntimeException {
    private static final long serialVersionUID = 1L;

    Exceeded() {
      super("regex step budget exceeded", null, false, false);
    }
  }

  private final CharSequence value;
  private long remaining;

  RegexBudget(CharSequence value) {
    this(value, BASE_STEPS + STEPS_PER_CHAR * value.length());
  }

  RegexBudget(CharSequence value, long steps) {
    this.value = value;
    this.remaining = steps;
  }

  @Override
  public char charAt(int index) {
    if (--remaining < 0) {
      throw new Exceeded();
    }
    return value.charAt(index);
  }

  @Override
  public int length() {
    return value.length();
  }

  @Override
  public CharSequence subSequence(int start, int end) {
    return value.subSequence(start, end);
  }

  @Override
  public String toString() {
    return value.toString();
  }
}
