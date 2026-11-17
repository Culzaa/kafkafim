package io.github.matabar.kafkafim.dlq;

import static io.github.matabar.kafkafim.GeneratedInputs.ROUNDS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.matabar.kafkafim.GeneratedInputs;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.Test;

/** Generated inputs for {@link DlqRecordFormat#truncateUtf8}. */
class DlqRecordFormatGeneratedInputTest {

  @Test
  void truncationNeverSplitsCodePointsAndKeepsPrefix() throws CharacterCodingException {
    Random random = new Random(317);
    for (int i = 0; i < ROUNDS * 5; i++) {
      String text = GeneratedInputs.text(random, 200, false);
      int max = 4 + random.nextInt(300);

      byte[] cut = DlqRecordFormat.truncateUtf8(text, max);

      byte[] full = text.getBytes(UTF_8);
      assertThat(cut.length).as("round %d max %d", i, max).isLessThanOrEqualTo(max);
      // strict decoding throws on a split sequence
      String decoded = UTF_8.newDecoder().decode(ByteBuffer.wrap(cut)).toString();
      assertThat(text).startsWith(decoded);
      assertThat(cut).isEqualTo(Arrays.copyOf(full, cut.length));
      if (full.length <= max) {
        assertThat(cut).isEqualTo(full);
      } else {
        // maximal: the next code point would not have fit
        int next = text.codePointAt(decoded.length());
        assertThat(cut.length + new String(Character.toChars(next)).getBytes(UTF_8).length)
            .isGreaterThan(max);
      }
    }
  }

  @Test
  void unpairedSurrogatesNeverBreakTruncation() throws CharacterCodingException {
    Random random = new Random(318);
    for (int i = 0; i < ROUNDS; i++) {
      String text = GeneratedInputs.text(random, 100, true);
      int max = 4 + random.nextInt(100);

      byte[] cut = DlqRecordFormat.truncateUtf8(text, max);

      assertThat(cut.length).isLessThanOrEqualTo(max);
      UTF_8.newDecoder().decode(ByteBuffer.wrap(cut));
    }
  }

  @Test
  void shortTextRoundTripsUnchanged() {
    Random random = new Random(319);
    for (int i = 0; i < ROUNDS; i++) {
      String text = GeneratedInputs.text(random, 100, false);
      byte[] cut = DlqRecordFormat.truncateUtf8(text, DlqRecordFormat.MAX_REASON_BYTES);
      assertThat(new String(cut, UTF_8)).isEqualTo(text);
    }
  }
}
