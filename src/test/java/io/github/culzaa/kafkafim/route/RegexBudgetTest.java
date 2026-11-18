package io.github.culzaa.kafkafim.route;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class RegexBudgetTest {

  @Test
  void countsCharAtCallsAndThrowsPastTheBudget() {
    RegexBudget budget = new RegexBudget("abc", 3);

    assertThat(budget.charAt(0)).isEqualTo('a');
    assertThat(budget.charAt(1)).isEqualTo('b');
    assertThat(budget.charAt(2)).isEqualTo('c');
    assertThatThrownBy(() -> budget.charAt(0)).isInstanceOf(RegexBudget.Exceeded.class);
  }

  @Test
  void delegatesLengthSubSequenceAndToString() {
    RegexBudget budget = new RegexBudget("abcd");

    assertThat(budget.length()).isEqualTo(4);
    assertThat(budget.subSequence(1, 3).toString()).isEqualTo("bc");
    assertThat(budget).hasToString("abcd");
  }

  @Test
  void theBudgetGrowsWithTheValue() {
    RegexBudget budget = new RegexBudget("x".repeat(100));

    // Reading length() does not spend steps.
    assertThat(budget.length()).isEqualTo(100);
    for (int i = 0; i < RegexBudget.BASE_STEPS / 1000; i++) {
      budget.charAt(0);
    }
    assertThat(budget.charAt(0)).isEqualTo('x');
  }

  @Test
  void exceededCarriesNoStackTrace() {
    assertThat(new RegexBudget.Exceeded().getStackTrace()).isEmpty();
  }
}
