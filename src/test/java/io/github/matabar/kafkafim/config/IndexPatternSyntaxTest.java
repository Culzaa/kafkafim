package io.github.matabar.kafkafim.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class IndexPatternSyntaxTest {

  @Test
  void nullAndBlankPatternsAreEmpty() {
    assertThat(IndexPatternSyntax.errors(null)).containsExactly("index pattern is empty");
    assertThat(IndexPatternSyntax.errors("  ")).containsExactly("index pattern is empty");
  }

  @Test
  void dotNamesAreRejected() {
    assertThat(IndexPatternSyntax.errors(".")).anyMatch(e -> e.contains("'.' or '..'"));
    assertThat(IndexPatternSyntax.errors("..")).anyMatch(e -> e.contains("'.' or '..'"));
  }

  @Test
  void unterminatedReferenceIsReported() {
    assertThat(IndexPatternSyntax.errors("logs-%{foo")).anyMatch(e -> e.contains("unterminated"));
  }

  @Test
  void whitespaceInFieldNameIsReported() {
    assertThat(IndexPatternSyntax.errors("logs-%{a b}"))
        .anyMatch(e -> e.contains("invalid field name"));
  }

  @Test
  void warningsForNullPatternAreEmpty() {
    assertThat(IndexPatternSyntax.warnings(null)).isEmpty();
  }

  @Test
  void leadingDotIsWarnedAboutButDotNamesAreNot() {
    assertThat(IndexPatternSyntax.warnings(".hidden-%{+yyyy}")).hasSize(1);
    assertThat(IndexPatternSyntax.warnings(".")).isEmpty();
  }

  @Test
  void weekBasedYearIsWarnedAboutButQuotedLetterIsNot() {
    assertThat(IndexPatternSyntax.warnings("logs-%{+YYYY}"))
        .anyMatch(w -> w.contains("week-based"));
    assertThat(IndexPatternSyntax.warnings("logs-%{+'Y'yyyy}")).isEmpty();
  }

  @Test
  void unterminatedDateReferenceStopsTheYearScan() {
    assertThat(IndexPatternSyntax.warnings("logs-%{+YYYY")).isEmpty();
  }

  @Test
  void invalidLeadingDatePatternIsNotWarnedAboutHere() {
    assertThat(IndexPatternSyntax.warnings("%{+qqqqqqqqqq}x")).isEmpty();
  }
}
