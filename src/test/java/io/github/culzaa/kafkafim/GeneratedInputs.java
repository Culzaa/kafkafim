package io.github.culzaa.kafkafim;

import java.nio.charset.StandardCharsets;
import java.util.Random;

/**
 * Seeded generators for the generated-input tests on the poison-message path. Every test builds its
 * own {@link Random} from a fixed seed, so a failure reproduces.
 */
public final class GeneratedInputs {

  /** Iterations per generated-input test: enough variety, still milliseconds. */
  public static final int ROUNDS = 2_000;

  private GeneratedInputs() {}

  /**
   * A string mixing 1, 2, 3 and 4 byte UTF-8 characters and, when {@code lone} is set, unpaired
   * surrogates.
   */
  public static String text(Random random, int maxLength, boolean lone) {
    int length = random.nextInt(maxLength + 1);
    StringBuilder out = new StringBuilder();
    while (out.length() < length) {
      switch (random.nextInt(lone ? 6 : 5)) {
        case 0, 1 -> out.append((char) ('a' + random.nextInt(26)));
        case 2 -> out.append((char) (0x80 + random.nextInt(0x700)));
        case 3 -> out.append((char) (0x800 + random.nextInt(0xD000 - 0x800)));
        case 4 -> out.appendCodePoint(0x10000 + random.nextInt(0x100000));
        default -> out.append((char) (0xD800 + random.nextInt(0x800)));
      }
    }
    return out.toString();
  }

  /** Random bytes, biased towards JSON punctuation, UTF-8 lead bytes and BOM bytes. */
  public static byte[] bytes(Random random, int maxLength) {
    byte[] pool = "{}[]\",:0123456789-+.eE truefalsn\\/\n".getBytes(StandardCharsets.UTF_8);
    byte[] out = new byte[random.nextInt(maxLength + 1)];
    for (int i = 0; i < out.length; i++) {
      out[i] =
          switch (random.nextInt(4)) {
            case 0 -> (byte) random.nextInt(256);
            case 1 -> (byte) (0xC0 + random.nextInt(0x40));
            default -> pool[random.nextInt(pool.length)];
          };
    }
    return out;
  }
}
