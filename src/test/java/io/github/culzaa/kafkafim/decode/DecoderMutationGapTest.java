package io.github.culzaa.kafkafim.decode;

import static io.github.culzaa.kafkafim.decode.DecoderTest.INGEST;
import static io.github.culzaa.kafkafim.decode.DecoderTest.recordFailure;
import static io.github.culzaa.kafkafim.decode.DecoderTest.single;
import static io.github.culzaa.kafkafim.decode.DecoderTest.singleFailure;
import static io.github.culzaa.kafkafim.decode.DecoderTest.spec;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import io.github.culzaa.kafkafim.decode.DecodeSpec.IdMissing;
import io.github.culzaa.kafkafim.decode.DecodeSpec.OnBadTimestamp;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Tests added after the first PIT run (docs/DELIVERY.md, "Mutation testing"): the id rules, the
 * non-UTF-8 detection, pointer checks and the length of quoted excerpts.
 */
class DecoderMutationGapTest {

  private static final Decoder DLQ_ON_BAD_TIMESTAMP =
      Decoder.compile(spec(IdMissing.DLQ, null, "/ts", "iso-8601", OnBadTimestamp.DLQ, 1 << 20));

  private static final Decoder DECODER =
      Decoder.compile(spec(IdMissing.DLQ, null, null, "iso-8601", OnBadTimestamp.DLQ, 1 << 20));

  @Test
  void decoderReturnsTheSpecItWasCompiledFrom() {
    DecodeSpec spec = spec(IdMissing.AUTO, "/v", null, "iso-8601", OnBadTimestamp.DLQ, 99);
    assertThat(Decoder.compile(spec).spec()).isSameAs(spec);
  }

  @Test
  void emptyStringIdIsRefused() {
    assertThat(singleFailure(DECODER, "{\"id\":\"\"}").reason()).contains("is an empty string");
  }

  @Test
  void stringIdIsLimitedByUtfEightBytesNotCharacters() {
    // 256 two-byte characters are 512 bytes: accepted. One more byte is not.
    assertThat(single(DECODER, "{\"id\":\"" + "é".repeat(256) + "\"}")).isNotNull();
    assertThat(singleFailure(DECODER, "{\"id\":\"" + "é".repeat(256) + "a\"}").reason())
        .contains("is longer than 512 bytes");
    assertThat(single(DECODER, "{\"id\":\"" + "a".repeat(512) + "\"}")).isNotNull();
    assertThat(singleFailure(DECODER, "{\"id\":\"" + "a".repeat(513) + "\"}").reason())
        .contains("is longer than 512 bytes");
    // Three-byte characters take the same path: 171 of them are 513 bytes.
    assertThat(singleFailure(DECODER, "{\"id\":\"" + "€".repeat(171) + "\"}").reason())
        .contains("is longer than 512 bytes");
    assertThat(single(DECODER, "{\"id\":\"" + "€".repeat(170) + "\"}")).isNotNull();
  }

  @Test
  void integerIdIsLimitedByItsDigits() {
    assertThat(single(DECODER, "{\"id\":" + "9".repeat(512) + "}")).isNotNull();
    assertThat(singleFailure(DECODER, "{\"id\":" + "9".repeat(513) + "}").reason())
        .contains("is longer than 512 bytes");
    assertThat(single(DECODER, "{\"id\":42}")).isNotNull();
  }

  @Test
  void fractionalOrOtherIdIsRefusedWithTheReason() {
    assertThat(singleFailure(DECODER, "{\"id\":1.5}").reason())
        .contains("is a number with a fraction or exponent; use a string or an integer");
    assertThat(singleFailure(DECODER, "{\"id\":true}").reason())
        .contains("is boolean, not a string or an integer");
    assertThat(singleFailure(DECODER, "{\"id\":{}}").reason())
        .contains("is object, not a string or an integer");
  }

  @Test
  void secondByteOrderMarkIsRefused() {
    byte[] value = "ï»¿".getBytes(StandardCharsets.ISO_8859_1);
    byte[] twice = new byte[value.length * 2 + 8];
    System.arraycopy(new byte[] {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF}, 0, twice, 0, 3);
    System.arraycopy(new byte[] {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF}, 0, twice, 3, 3);
    System.arraycopy("{\"id\":1}".getBytes(StandardCharsets.UTF_8), 0, twice, 6, 8);
    assertThat(recordFailure(DECODER, twice).reason())
        .isEqualTo("record value starts with more than one byte order mark");
  }

  @Test
  void utf16AndNulBytesAreRefusedWhereverTheyAppearInTheFirstFour() {
    assertThat(recordFailure(DECODER, new byte[] {(byte) 0xFF, (byte) 0xFE, '{', 0}).reason())
        .isEqualTo("record value is not UTF-8 (UTF-16/32 byte order mark)");
    assertThat(recordFailure(DECODER, new byte[] {(byte) 0xFE, (byte) 0xFF, 0, '{'}).reason())
        .isEqualTo("record value is not UTF-8 (UTF-16/32 byte order mark)");
    String nul = "record value is not UTF-8 (NUL byte; UTF-16/32?)";
    assertThat(recordFailure(DECODER, new byte[] {'{', 0, '"', 'a'}).reason()).isEqualTo(nul);
    assertThat(recordFailure(DECODER, new byte[] {'{', '"', 'a', 0}).reason()).isEqualTo(nul);
    // The same check applies after a single byte order mark.
    assertThat(
            recordFailure(DECODER, new byte[] {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF, '{', 0, '"'})
                .reason())
        .isEqualTo(nul);
  }

  @Test
  void pointerEndingInTildeIsRefusedAtCompileTime() {
    DecodeSpec spec = spec(IdMissing.DLQ, null, null, "iso-8601", OnBadTimestamp.DLQ, 10);
    DecodeSpec bad =
        new DecodeSpec("/a~", spec.idMissing(), null, null, "iso-8601", OnBadTimestamp.DLQ, 10);
    assertThatIllegalArgumentException()
        .isThrownBy(() -> Decoder.compile(bad))
        .withMessageContaining("'~' must be followed by '0' or '1'");
    DecodeSpec escaped =
        new DecodeSpec("/a~1b", spec.idMissing(), null, null, "iso-8601", OnBadTimestamp.DLQ, 10);
    assertThat(Decoder.compile(escaped)).isNotNull();
  }

  @Test
  void badTimestampQuotesAtMostSixtyFourCharacters() {
    String exact = "x".repeat(64);
    String longer = "x".repeat(65);
    assertThat(singleFailure(DLQ_ON_BAD_TIMESTAMP, "{\"id\":1,\"ts\":\"" + exact + "\"}").reason())
        .contains("'" + exact + "' is not an ISO-8601")
        .doesNotContain("...");
    assertThat(singleFailure(DLQ_ON_BAD_TIMESTAMP, "{\"id\":1,\"ts\":\"" + longer + "\"}").reason())
        .contains("'" + exact + "...' is not an ISO-8601");
    assertThat(singleFailure(DLQ_ON_BAD_TIMESTAMP, "{\"id\":1,\"ts\":[]}").reason())
        .contains("array is not an ISO-8601 string");
  }

  @Test
  void documentWithoutTimestampFieldUsesTheIngestTime() {
    assertThat(single(DECODER, "{\"id\":1}").timestamp()).isEqualTo(INGEST);
  }
}
