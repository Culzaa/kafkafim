package io.github.matabar.kafkafim.decode;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/** The framework of route transforms with test ops: the real ops come with their own tests. */
class DocumentTransformTest {

  private static final JsonMapper MAPPER = JsonMapper.builder().build();

  /** Returns its input untouched. */
  static final StepRegistry.Factory NOOP =
      spec ->
          new TransformStep() {
            @Override
            public JsonNode apply(JsonNode root) {
              return root;
            }

            @Override
            public String describe() {
              return "noop";
            }
          };

  /** Appends {@code <field>: <value>} to the root, copy on write. */
  static final StepRegistry.Factory APPEND =
      spec ->
          new TransformStep() {
            @Override
            public JsonNode apply(JsonNode root) {
              ObjectNode copy = ((ObjectNode) root).deepCopy();
              copy.put(spec.field(), spec.value());
              return copy;
            }

            @Override
            public String describe() {
              return "append " + spec.field();
            }
          };

  /** Always fails. */
  static final StepRegistry.Factory FAIL =
      spec ->
          new TransformStep() {
            @Override
            public JsonNode apply(JsonNode root) throws TransformException {
              throw new TransformException("parent is not an object");
            }

            @Override
            public String describe() {
              return "fail " + spec.field();
            }
          };

  /** Wraps the root in {@code spec.value()} levels of objects. */
  static final StepRegistry.Factory WRAP =
      spec ->
          new TransformStep() {
            @Override
            public JsonNode apply(JsonNode root) {
              JsonNode current = root;
              for (int i = 0; i < Integer.parseInt(spec.value()); i++) {
                current = MAPPER.createObjectNode().set("a", current);
              }
              return current;
            }

            @Override
            public String describe() {
              return "wrap";
            }

            @Override
            public boolean deepens() {
              return true;
            }
          };

  static final StepRegistry REGISTRY =
      StepRegistry.of(Map.of("noop", NOOP, "append", APPEND, "fail", FAIL, "wrap", WRAP));

  private static TransformStepSpec step(String op) {
    return new TransformStepSpec(op, null, null, null, null);
  }

  private static TransformStepSpec append(String field, String value) {
    return new TransformStepSpec("append", field, null, value, null);
  }

  private static DocumentSource source(String json) {
    return DocumentSource.of(json.getBytes(UTF_8), 0, json.getBytes(UTF_8).length);
  }

  private static String transform(String route, List<TransformStepSpec> steps, String json)
      throws TransformException {
    DocumentTransform transform = DocumentTransform.compile(route, steps, REGISTRY);
    DocumentSource out = transform.apply(DocumentTransform.parseExact(source(json)));
    return new String(out.toByteArray(), UTF_8);
  }

  @Test
  void noStepsMeansNoTransformAndTheZeroCopyPath() {
    assertThat(DocumentTransform.compile("r", List.of(), REGISTRY)).isNull();
    assertThat(DocumentTransform.compile("r", List.of())).isNull();
  }

  @Test
  void unknownOpFailsCompilationWithTheRouteAndPosition() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> DocumentTransform.compile("eu", List.of(step("noop"), step("bogus")), REGISTRY))
        .withMessage("route eu, step 2: unknown op 'bogus'");
    assertThatIllegalArgumentException()
        .isThrownBy(() -> DocumentTransform.compile("eu", List.of(step("noop"))))
        .withMessageContaining("unknown op 'noop'");
  }

  @Test
  void noopStepRewritesTheDocumentAsCompactOneLineJson() throws Exception {
    String pretty = "{\n  \"a\" : 1,\n  \"b\" : { \"c\" : [ 1, 2 ] },\n  \"s\" : \"x y\"\n}";
    assertThat(transform("r", List.of(step("noop")), pretty))
        .isEqualTo("{\"a\":1,\"b\":{\"c\":[1,2]},\"s\":\"x y\"}");
  }

  @Test
  void keepsKeyOrderAndAppendsNewMembersAtTheEnd() throws Exception {
    assertThat(transform("r", List.of(append("z", "1")), "{\"m\":1,\"a\":0}"))
        .isEqualTo("{\"m\":1,\"a\":0,\"z\":\"1\"}");
  }

  @Test
  void numbersKeepTheirExactValue() throws Exception {
    String json =
        "{\"big\":123456789012345678901234567890,\"dec\":0.1234567890123456789012345,"
            + "\"keep\":1.10,\"exp\":1e3,\"neg\":-0,\"negdec\":-0.0}";
    assertThat(transform("r", List.of(step("noop")), json))
        .isEqualTo(
            "{\"big\":123456789012345678901234567890,\"dec\":0.1234567890123456789012345,"
                + "\"keep\":1.10,\"exp\":1E+3,\"neg\":0,\"negdec\":0.0}");
  }

  @Test
  void stringsUseMinimalEscapesAndKeepUnicode() throws Exception {
    assertThat(transform("r", List.of(step("noop")), "{\"s\":\"\\u00e9\\n\\/\\u0001\"}"))
        .isEqualTo("{\"s\":\"é\\n/\\u0001\"}");
  }

  @Test
  void stepsRunInOrderAndDoNotMutateTheSharedTree() throws Exception {
    DocumentTransform transform =
        DocumentTransform.compile("r", List.of(append("x", "1"), append("y", "2")), REGISTRY);
    JsonNode exact = DocumentTransform.parseExact(source("{\"a\":1}"));
    DocumentSource first = transform.apply(exact);
    DocumentSource second = transform.apply(exact);
    assertThat(new String(first.toByteArray(), UTF_8))
        .isEqualTo("{\"a\":1,\"x\":\"1\",\"y\":\"2\"}");
    assertThat(new String(second.toByteArray(), UTF_8))
        .isEqualTo(new String(first.toByteArray(), UTF_8));
    assertThat(exact.toString()).isEqualTo("{\"a\":1}");
  }

  @Test
  void resultOwnsItsBytesAndWritesThem() throws Exception {
    DocumentTransform transform = DocumentTransform.compile("r", List.of(step("noop")), REGISTRY);
    DocumentSource out = transform.apply(DocumentTransform.parseExact(source("{\"a\": 1}")));
    ByteArrayOutputStream sink = new ByteArrayOutputStream();
    out.writeTo(sink);
    assertThat(sink.toString(UTF_8)).isEqualTo("{\"a\":1}");
    assertThat(out.length()).isEqualTo(7);
    assertThat(out.asByteBuffer().remaining()).isEqualTo(7);
    assertThat(out.asByteBuffer().isReadOnly()).isTrue();
  }

  @Test
  void failingStepNamesTheRouteThePositionAndTheOpButNoValue() {
    DocumentTransform transform =
        DocumentTransform.compile(
            "eu",
            List.of(step("noop"), new TransformStepSpec("fail", "/a/b", null, null, null)),
            REGISTRY);
    assertThatThrownBy(
            () -> transform.apply(DocumentTransform.parseExact(source("{\"a\":\"secret\"}"))))
        .isInstanceOf(TransformException.class)
        .hasMessage("route eu, step 2 (fail /a/b): parent is not an object");
  }

  @Test
  void deepeningStepFailsBeyondTheNestingLimit() throws Exception {
    DocumentTransform ok =
        DocumentTransform.compile(
            "r", List.of(new TransformStepSpec("wrap", null, null, "499", null)), REGISTRY);
    JsonNode exact = DocumentTransform.parseExact(source("{\"x\":1}"));
    assertThat(ok.apply(exact).length()).isPositive(); // 500 levels

    DocumentTransform tooDeep =
        DocumentTransform.compile(
            "r", List.of(new TransformStepSpec("wrap", null, null, "500", null)), REGISTRY);
    assertThatThrownBy(() -> tooDeep.apply(exact))
        .isInstanceOf(TransformException.class)
        .hasMessage("route r, step 1 (wrap): the document would nest deeper than 500");
  }
}
