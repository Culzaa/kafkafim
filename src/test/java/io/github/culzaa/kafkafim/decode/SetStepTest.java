package io.github.culzaa.kafkafim.decode;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

/** The {@code set} op through the production registry (DESIGN §8, ADR-0047). */
class SetStepTest {

  private static DocumentSource source(String json) {
    byte[] bytes = json.getBytes(UTF_8);
    return DocumentSource.of(bytes, 0, bytes.length);
  }

  private static TransformStepSpec value(String field, String value) {
    return new TransformStepSpec("set", field, null, value, null);
  }

  private static TransformStepSpec json(String field, String json) {
    return new TransformStepSpec("set", field, null, null, json);
  }

  private static String run(String json, TransformStepSpec... steps) throws TransformException {
    DocumentTransform transform = DocumentTransform.compile("r", List.of(steps));
    return new String(
        transform.apply(DocumentTransform.parseExact(source(json))).toByteArray(), UTF_8);
  }

  @Test
  void newMemberGoesLastAndCreatesMissingParents() throws Exception {
    assertThat(run("{\"a\":1}", value("/b", "x"))).isEqualTo("{\"a\":1,\"b\":\"x\"}");
    assertThat(run("{\"a\":1}", value("/event/dataset", "orders")))
        .isEqualTo("{\"a\":1,\"event\":{\"dataset\":\"orders\"}}");
    assertThat(run("{\"e\":{\"k\":0},\"z\":1}", value("/e/x/y", "v")))
        .isEqualTo("{\"e\":{\"k\":0,\"x\":{\"y\":\"v\"}},\"z\":1}");
  }

  @Test
  void existingMemberIsOverwrittenInPlaceAtAnyDepth() throws Exception {
    assertThat(run("{\"a\":1,\"b\":2,\"c\":3}", value("/b", "x")))
        .isEqualTo("{\"a\":1,\"b\":\"x\",\"c\":3}");
    assertThat(run("{\"s\":0,\"t\":{\"p\":1,\"q\":2,\"r\":3}}", json("/t/q", "9")))
        .isEqualTo("{\"s\":0,\"t\":{\"p\":1,\"q\":9,\"r\":3}}");
  }

  @Test
  void overwritesWhateverTypeTheMemberHad() throws Exception {
    assertThat(run("{\"a\":{\"deep\":[1]},\"n\":null}", json("/a", "1"), value("/n", "x")))
        .isEqualTo("{\"a\":1,\"n\":\"x\"}");
  }

  @Test
  void valueIsJsonStringEvenWhenItLooksLikeAnotherType() throws Exception {
    assertThat(
            run("{}", value("/a", "2"), value("/b", "true"), value("/c", "null"), value("/d", "")))
        .isEqualTo("{\"a\":\"2\",\"b\":\"true\",\"c\":\"null\",\"d\":\"\"}");
    assertThat(run("{}", value("/q", "a\"b\\cé"))).isEqualTo("{\"q\":\"a\\\"b\\\\cé\"}");
  }

  @Test
  void jsonWritesAnyJsonValue() throws Exception {
    assertThat(
            run(
                "{}",
                json("/n", "2"),
                json("/t", "true"),
                json("/z", "null"),
                json("/s", "\"x\""),
                json("/o", "{ \"k\" : [1, {\"m\": 2.50}] }"),
                json("/arr", "[]")))
        .isEqualTo(
            "{\"n\":2,\"t\":true,\"z\":null,\"s\":\"x\","
                + "\"o\":{\"k\":[1,{\"m\":2.50}]},\"arr\":[]}");
  }

  @Test
  void jsonKeepsExactNumbers() throws Exception {
    assertThat(run("{}", json("/big", "123456789012345678901234567890"), json("/d", "0.10")))
        .isEqualTo("{\"big\":123456789012345678901234567890,\"d\":0.10}");
  }

  @Test
  void parentThatIsNotAnObjectFailsTheCopyWithoutValues() {
    for (String doc : List.of("{\"x\":\"secret\"}", "{\"x\":null}", "{\"x\":[1]}", "{\"x\":7}")) {
      assertThatThrownBy(() -> run(doc, value("/x/y", "v")))
          .isInstanceOf(TransformException.class)
          .hasMessage("route r, step 1 (set /x/y): a parent of the field is not an object");
    }
    assertThatThrownBy(() -> run("{\"x\":{\"y\":1}}", value("/x/y/z", "v")))
        .isInstanceOf(TransformException.class);
  }

  @Test
  void tokensAreObjectKeysNeverArrayIndexes() throws Exception {
    assertThat(run("{\"a\":{\"0\":1}}", value("/a/0", "x"))).isEqualTo("{\"a\":{\"0\":\"x\"}}");
    assertThatThrownBy(() -> run("{\"a\":[1]}", value("/a/0", "x")))
        .isInstanceOf(TransformException.class);
  }

  @Test
  void escapedKeys() throws Exception {
    assertThat(run("{}", value("/a~1b", "x"), value("/c~0d", "y")))
        .isEqualTo("{\"a/b\":\"x\",\"c~d\":\"y\"}");
  }

  @Test
  void stepsChainAndLaterStepsReadWhatEarlierOnesWrote() throws Exception {
    assertThat(
            run(
                "{\"a\":1}",
                value("/x", "1"),
                new TransformStepSpec("rename", "/x", "/y/x", null, null),
                value("/y/z", "2"),
                new TransformStepSpec("remove", "/a", null, null, null)))
        .isEqualTo("{\"y\":{\"x\":\"1\",\"z\":\"2\"}}");
  }

  @Test
  void doesNotMutateTheSharedTreeOrTheConstantSoCopiesAreIndependent() throws Exception {
    JsonNode shared = DocumentTransform.parseExact(source("{\"id\":1,\"k\":{\"m\":3}}"));
    String before = shared.toString();
    DocumentTransform one =
        DocumentTransform.compile("a", List.of(json("/k/n", "{\"c\":1}"), value("/id", "x")));
    DocumentTransform two = DocumentTransform.compile("b", List.of(value("/k/m", "y")));

    String first = new String(one.apply(shared).toByteArray(), UTF_8);
    String firstAgain = new String(one.apply(shared).toByteArray(), UTF_8);
    String second = new String(two.apply(shared).toByteArray(), UTF_8);

    assertThat(shared.toString()).isEqualTo(before);
    assertThat(first).isEqualTo("{\"id\":\"x\",\"k\":{\"m\":3,\"n\":{\"c\":1}}}");
    assertThat(firstAgain).isEqualTo(first);
    assertThat(second).isEqualTo("{\"id\":1,\"k\":{\"m\":\"y\"}}");
  }

  @Test
  void nonObjectRootIsLeftAlone() throws Exception {
    JsonNode array = DocumentTransform.parseExact(source("[1]"));
    TransformStep step = StepRegistry.BUILT_IN.factory("set").compile(value("/a", "x"));
    assertThat(step.apply(array)).isSameAs(array);
    assertThat(step.describe()).isEqualTo("set /a");
  }

  @Test
  void compileRejectsStepWithoutConstantOrWithBadJson() {
    StepRegistry.Factory set = StepRegistry.BUILT_IN.factory("set");
    assertThatThrownBy(() -> set.compile(new TransformStepSpec("set", "/a", null, null, null)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> set.compile(json("/a", "{")))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void declaresThatItCanDeepenTheDocument() {
    assertThat(StepRegistry.BUILT_IN.factory("set").compile(value("/a", "x")).deepens()).isTrue();
  }

  private static String path(int keys) {
    StringBuilder pointer = new StringBuilder();
    for (int i = 1; i <= keys; i++) {
      pointer.append("/k").append(i);
    }
    return pointer.toString();
  }

  @Test
  void failsWhenCreatingParentsNestsTheDocumentDeeperThan500() throws Exception {
    assertThat(run("{}", value(path(500), "x")).length()).isPositive(); // 500 levels: allowed
    String target = path(501);
    assertThatThrownBy(() -> run("{}", value(target, "x")))
        .isInstanceOf(TransformException.class)
        .hasMessage(
            "route r, step 1 (set " + target + "): the document would nest deeper than 500");
  }

  @Test
  void failsWhenNestedJsonConstantNestsTheDocumentDeeperThan500() throws Exception {
    String constant = "[".repeat(490) + "]".repeat(490);
    assertThat(run("{}", json(path(9), constant)).length()).isPositive(); // 9 + 490 + 1 levels
    String target = path(11);
    assertThatThrownBy(() -> run("{}", json(target, constant)))
        .isInstanceOf(TransformException.class)
        .hasMessage(
            "route r, step 1 (set " + target + "): the document would nest deeper than 500");
  }

  @Test
  void theProductionRegistryKnowsSet() {
    assertThat(StepRegistry.BUILT_IN.ops()).containsExactlyInAnyOrder("remove", "rename", "set");
  }
}
