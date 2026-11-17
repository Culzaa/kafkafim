package io.github.matabar.kafkafim.decode;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

/** The {@code rename} op through the production registry (DESIGN §8, ADR-0047). */
class RenameStepTest {

  private static DocumentSource source(String json) {
    byte[] bytes = json.getBytes(UTF_8);
    return DocumentSource.of(bytes, 0, bytes.length);
  }

  private static TransformStepSpec rename(String field, String to) {
    return new TransformStepSpec("rename", field, to, null, null);
  }

  private static String run(String json, TransformStepSpec... steps) throws TransformException {
    DocumentTransform transform = DocumentTransform.compile("r", List.of(steps));
    return new String(
        transform.apply(DocumentTransform.parseExact(source(json))).toByteArray(), UTF_8);
  }

  @Test
  void movesTopLevelMemberToTheEnd() throws Exception {
    assertThat(run("{\"usr\":1,\"b\":2}", rename("/usr", "/user")))
        .isEqualTo("{\"b\":2,\"user\":1}");
  }

  @Test
  void movesNestedToNested() throws Exception {
    assertThat(run("{\"a\":{\"x\":1,\"y\":2},\"b\":{\"z\":3}}", rename("/a/x", "/b/w")))
        .isEqualTo("{\"a\":{\"y\":2},\"b\":{\"z\":3,\"w\":1}}");
  }

  @Test
  void movesBetweenLevels() throws Exception {
    assertThat(run("{\"a\":{\"x\":1},\"k\":2}", rename("/a/x", "/x")))
        .isEqualTo("{\"a\":{},\"k\":2,\"x\":1}");
    assertThat(run("{\"x\":1,\"k\":2}", rename("/x", "/a/x")))
        .isEqualTo("{\"k\":2,\"a\":{\"x\":1}}");
  }

  @Test
  void missingSourceChangesNothing() throws Exception {
    String json = "{\"a\":{\"b\":1}}";
    assertThat(run(json, rename("/x", "/y"))).isEqualTo(json);
    assertThat(run(json, rename("/a/x", "/y"))).isEqualTo(json);
    assertThat(run(json, rename("/x/y/z", "/q"))).isEqualTo(json);
  }

  @Test
  void missingSourceIsNoOpEvenWhenTheTargetParentIsNotAnObject() throws Exception {
    String json = "{\"s\":\"text\"}";
    assertThat(run(json, rename("/x", "/s/y"))).isEqualTo(json);
  }

  @Test
  void sourceParentThatIsNotAnObjectIsNoOp() throws Exception {
    String json = "{\"s\":\"text\",\"n\":null,\"arr\":[{\"a\":1}]}";
    assertThat(
            run(
                json,
                rename("/s/a", "/t"),
                rename("/n/a", "/t"),
                rename("/arr/a", "/t"),
                rename("/arr/0/a", "/t")))
        .isEqualTo(json);
  }

  @Test
  void existingTargetIsOverwrittenInPlace() throws Exception {
    assertThat(run("{\"a\":1,\"user\":\"old\",\"usr\":2,\"z\":3}", rename("/usr", "/user")))
        .isEqualTo("{\"a\":1,\"user\":2,\"z\":3}");
  }

  @Test
  void existingNestedTargetKeepsItsPosition() throws Exception {
    assertThat(run("{\"s\":7,\"t\":{\"p\":1,\"q\":2,\"r\":3}}", rename("/s", "/t/q")))
        .isEqualTo("{\"t\":{\"p\":1,\"q\":7,\"r\":3}}");
  }

  @Test
  void createsMissingParentsOfTheTarget() throws Exception {
    assertThat(run("{\"a\":1}", rename("/a", "/x/y/z"))).isEqualTo("{\"x\":{\"y\":{\"z\":1}}}");
    assertThat(run("{\"a\":1,\"x\":{\"k\":0}}", rename("/a", "/x/y/z")))
        .isEqualTo("{\"x\":{\"k\":0,\"y\":{\"z\":1}}}");
  }

  @Test
  void targetParentThatIsNotObjectFailsTheCopyWithoutValues() {
    for (String json :
        List.of("{\"a\":1,\"x\":\"secret\"}", "{\"a\":1,\"x\":null}", "{\"a\":1,\"x\":[1]}")) {
      assertThatThrownBy(() -> run(json, rename("/a", "/x/y")))
          .isInstanceOf(TransformException.class)
          .hasMessage(
              "route r, step 1 (rename /a to /x/y): a parent of the target is not an object");
    }
    assertThatThrownBy(() -> run("{\"a\":1,\"x\":{\"y\":2}}", rename("/a", "/x/y/z")))
        .isInstanceOf(TransformException.class);
  }

  @Test
  void nullMemberIsPresentAndMoves() throws Exception {
    assertThat(run("{\"a\":null,\"b\":{}}", rename("/a", "/b/c")))
        .isEqualTo("{\"b\":{\"c\":null}}");
  }

  @Test
  void movesObjectsAndArraysWholeAndEscapedKeys() throws Exception {
    assertThat(run("{\"a/b\":{\"k\":[1,2]},\"c~d\":0}", rename("/a~1b", "/c~0d")))
        .isEqualTo("{\"c~d\":{\"k\":[1,2]}}");
    assertThat(run("{\"\":5}", rename("/", "/e"))).isEqualTo("{\"e\":5}");
  }

  @Test
  void tokensAreObjectKeysNeverArrayIndexes() throws Exception {
    String json = "{\"b\":[1,2],\"a\":{\"0\":1}}";
    assertThat(run(json, rename("/b/0", "/c"))).isEqualTo(json);
    assertThat(run(json, rename("/a/0", "/c"))).isEqualTo("{\"b\":[1,2],\"a\":{},\"c\":1}");
  }

  @Test
  void stepsChainEachOnTheResultOfThePreviousOne() throws Exception {
    assertThat(run("{\"a\":1}", rename("/a", "/b"), rename("/b", "/c/d"), rename("/c/d", "/e")))
        .isEqualTo("{\"c\":{},\"e\":1}");
  }

  @Test
  void exactNumbersSurvive() throws Exception {
    assertThat(run("{\"big\":123456789012345678901234567890}", rename("/big", "/n")))
        .isEqualTo("{\"n\":123456789012345678901234567890}");
  }

  @Test
  void doesNotMutateTheSharedTreeSoSiblingCopiesAreIndependent() throws Exception {
    JsonNode shared =
        DocumentTransform.parseExact(source("{\"id\":1,\"card\":{\"n\":2},\"k\":{\"m\":3}}"));
    String before = shared.toString();
    DocumentTransform one = DocumentTransform.compile("a", List.of(rename("/card/n", "/k/n")));
    DocumentTransform two = DocumentTransform.compile("b", List.of(rename("/k", "/card/k")));

    String first = new String(one.apply(shared).toByteArray(), UTF_8);
    String second = new String(two.apply(shared).toByteArray(), UTF_8);

    assertThat(shared.toString()).isEqualTo(before);
    assertThat(first).isEqualTo("{\"id\":1,\"card\":{},\"k\":{\"m\":3,\"n\":2}}");
    assertThat(second).isEqualTo("{\"id\":1,\"card\":{\"n\":2,\"k\":{\"m\":3}}}");
  }

  @Test
  void noOpReturnsTheSameTree() throws Exception {
    JsonNode root = DocumentTransform.parseExact(source("{\"a\":1}"));
    TransformStep step = StepRegistry.BUILT_IN.factory("rename").compile(rename("/zzz", "/y"));
    assertThat(step.apply(root)).isSameAs(root);
    assertThat(step.describe()).isEqualTo("rename /zzz to /y");
  }

  @Test
  void nonObjectRootIsLeftAlone() throws Exception {
    JsonNode array = DocumentTransform.parseExact(source("[1]"));
    TransformStep step = StepRegistry.BUILT_IN.factory("rename").compile(rename("/a", "/b"));
    assertThat(step.apply(array)).isSameAs(array);
  }

  @Test
  void declaresThatItCanDeepenTheDocument() {
    assertThat(StepRegistry.BUILT_IN.factory("rename").compile(rename("/a", "/b")).deepens())
        .isTrue();
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
    // "m" holds 490 nested objects; the target with k keys puts them k levels lower.
    String json = "{\"m\":" + "{\"a\":".repeat(489) + "{}" + "}".repeat(489) + "}";
    assertThat(run(json, rename("/m", path(10))).length()).isPositive(); // 500 levels: allowed
    String target = path(11); // 501 levels
    assertThatThrownBy(() -> run(json, rename("/m", target)))
        .isInstanceOf(TransformException.class)
        .hasMessage(
            "route r, step 1 (rename /m to "
                + target
                + "): "
                + "the document would nest deeper than 500");
  }

  @Test
  void theProductionRegistryKnowsRename() {
    assertThat(StepRegistry.BUILT_IN.ops()).contains("rename");
  }
}
