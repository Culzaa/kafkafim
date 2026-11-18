package io.github.culzaa.kafkafim.decode;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

/** The {@code remove} op through the production registry (DESIGN §8, ADR-0047). */
class RemoveStepTest {

  private static DocumentSource source(String json) {
    byte[] bytes = json.getBytes(UTF_8);
    return DocumentSource.of(bytes, 0, bytes.length);
  }

  private static TransformStepSpec remove(String field) {
    return new TransformStepSpec("remove", field, null, null, null);
  }

  private static String run(String json, String... fields) throws TransformException {
    DocumentTransform transform =
        DocumentTransform.compile(
            "r", java.util.Arrays.stream(fields).map(RemoveStepTest::remove).toList());
    return new String(
        transform.apply(DocumentTransform.parseExact(source(json))).toByteArray(), UTF_8);
  }

  @Test
  void removesOneMemberAndKeepsTheOrderOfTheRest() throws Exception {
    assertThat(run("{\"a\":1,\"b\":2,\"c\":3}", "/b")).isEqualTo("{\"a\":1,\"c\":3}");
  }

  @Test
  void removesNestedMember() throws Exception {
    assertThat(run("{\"id\":1,\"card\":{\"number\":\"4111\",\"brand\":\"v\"}}", "/card/number"))
        .isEqualTo("{\"id\":1,\"card\":{\"brand\":\"v\"}}");
  }

  @Test
  void removingTheLastMemberLeavesAnEmptyObject() throws Exception {
    assertThat(run("{\"card\":{\"number\":1}}", "/card/number")).isEqualTo("{\"card\":{}}");
  }

  @Test
  void missingPathChangesNothing() throws Exception {
    String json = "{\"a\":{\"b\":1}}";
    assertThat(run(json, "/x")).isEqualTo(json);
    assertThat(run(json, "/a/x")).isEqualTo(json);
    assertThat(run(json, "/x/y/z")).isEqualTo(json);
  }

  @Test
  void parentThatIsNotObjectChangesNothingAndDoesNotFail() throws Exception {
    String json = "{\"s\":\"text\",\"n\":null,\"arr\":[{\"a\":1}]}";
    assertThat(run(json, "/s/a", "/n/a", "/arr/a", "/arr/0/a")).isEqualTo(json);
  }

  @Test
  void tokensAreObjectKeysNeverArrayIndexes() throws Exception {
    assertThat(run("{\"a\":{\"0\":1,\"1\":2},\"b\":[1,2]}", "/a/0", "/b/0"))
        .isEqualTo("{\"a\":{\"1\":2},\"b\":[1,2]}");
  }

  @Test
  void nullMemberIsPresentAndRemoved() throws Exception {
    assertThat(run("{\"a\":null,\"b\":{\"c\":null}}", "/a", "/b/c")).isEqualTo("{\"b\":{}}");
  }

  @Test
  void removesTheMemberOfAnEscapedPointer() throws Exception {
    assertThat(run("{\"a/b\":1,\"c~d\":2,\"\":3,\"e\":4}", "/a~1b", "/c~0d", "/"))
        .isEqualTo("{\"e\":4}");
  }

  @Test
  void stepsRunInOrderEachOnTheResultOfThePreviousOne() throws Exception {
    assertThat(run("{\"a\":{\"b\":1,\"c\":2}}", "/a/b", "/a/b", "/a/c", "/a")).isEqualTo("{}");
  }

  @Test
  void doesNotMutateTheSharedTreeSoSiblingCopiesAreIndependent() throws Exception {
    JsonNode shared = DocumentTransform.parseExact(source("{\"id\":1,\"card\":{\"n\":2},\"k\":3}"));
    String before = shared.toString();
    DocumentTransform removing = DocumentTransform.compile("a", List.of(remove("/card/n")));
    DocumentTransform plainStamp = DocumentTransform.compile("b", List.of(remove("/k")));

    String first = new String(removing.apply(shared).toByteArray(), UTF_8);
    String second = new String(plainStamp.apply(shared).toByteArray(), UTF_8);

    assertThat(shared.toString()).isEqualTo(before);
    assertThat(first).isEqualTo("{\"id\":1,\"card\":{},\"k\":3}");
    assertThat(second).isEqualTo("{\"id\":1,\"card\":{\"n\":2}}");
  }

  @Test
  void untouchedDocumentReturnsTheSameTree() throws Exception {
    JsonNode root = DocumentTransform.parseExact(source("{\"a\":1}"));
    TransformStep step = StepRegistry.BUILT_IN.factory("remove").compile(remove("/zzz"));
    assertThat(step.apply(root)).isSameAs(root);
    assertThat(step.deepens()).isFalse();
    assertThat(step.describe()).isEqualTo("remove /zzz");
  }

  @Test
  void exactNumbersSurviveTheCopy() throws Exception {
    assertThat(run("{\"big\":123456789012345678901234567890,\"x\":1}", "/x"))
        .isEqualTo("{\"big\":123456789012345678901234567890}");
  }

  @Test
  void theProductionRegistryKnowsRemove() {
    assertThat(StepRegistry.BUILT_IN.ops()).contains("remove");
  }
}
