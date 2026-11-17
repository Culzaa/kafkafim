package io.github.matabar.kafkafim.sink.es;

import static io.github.matabar.kafkafim.GeneratedInputs.ROUNDS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.matabar.kafkafim.GeneratedInputs;
import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

/** Generated inputs for {@link BulkResponse#parse}: only the documented exception, round trips. */
class BulkResponseGeneratedInputTest {

  private static final JsonMapper JSON = JsonMapper.builder().build();

  @Test
  void arbitraryBytesOnlyEverThrowMalformedResponse() {
    Random random = new Random(320);
    for (int i = 0; i < ROUNDS * 5; i++) {
      parseOrMalformed(GeneratedInputs.bytes(random, 120));
    }
  }

  @Test
  void mutatedValidResponsesOnlyEverThrowMalformedResponse() {
    Random random = new Random(321);
    for (int i = 0; i < ROUNDS * 5; i++) {
      byte[] body = response(random, 1 + random.nextInt(6)).toString().getBytes(UTF_8);
      switch (random.nextInt(3)) {
        case 0 -> body[random.nextInt(body.length)] = (byte) random.nextInt(256);
        case 1 -> body = Arrays.copyOf(body, random.nextInt(body.length));
        default -> body[random.nextInt(body.length)] = (byte) "{}[]\",:".charAt(random.nextInt(7));
      }
      parseOrMalformed(body);
    }
  }

  @Test
  void itemsRoundTripByPosition() {
    Random random = new Random(322);
    for (int i = 0; i < ROUNDS; i++) {
      int size = 1 + random.nextInt(10);
      ObjectNode response = JSON.createObjectNode().put("errors", true);
      ArrayNode items = response.putArray("items");
      int[] statuses = new int[size];
      String[] types = new String[size];
      for (int n = 0; n < size; n++) {
        statuses[n] = random.nextBoolean() ? 201 : 400 + random.nextInt(200);
        types[n] = statuses[n] < 300 ? null : GeneratedInputs.text(random, 20, false);
        ObjectNode body = items.addObject().putObject(random.nextBoolean() ? "index" : "create");
        body.put("status", statuses[n]);
        if (types[n] != null) {
          body.putObject("error").put("type", types[n]).put("reason", "r");
        }
      }

      BulkResponse parsed = BulkResponse.parse(response.toString().getBytes(UTF_8));

      assertThat(parsed.errors()).isTrue();
      assertThat(parsed.items()).hasSize(size);
      for (int n = 0; n < size; n++) {
        assertThat(parsed.items().get(n).status()).isEqualTo(statuses[n]);
        assertThat(parsed.items().get(n).errorType()).isEqualTo(types[n]);
        if (types[n] != null) {
          assertThat(JSON.readTree(parsed.items().get(n).errorJson()).get("type").stringValue())
              .isEqualTo(types[n]);
        }
      }
    }
  }

  private static ObjectNode response(Random random, int size) {
    ObjectNode response = JSON.createObjectNode().put("errors", true);
    ArrayNode items = response.putArray("items");
    for (int n = 0; n < size; n++) {
      ObjectNode body = items.addObject().putObject("index");
      body.put("status", random.nextInt(600));
      if (random.nextBoolean()) {
        body.putObject("error").put("type", GeneratedInputs.text(random, 8, false));
      }
    }
    return response;
  }

  private static void parseOrMalformed(byte[] body) {
    try {
      BulkResponse parsed = BulkResponse.parse(body);
      assertThat(parsed.items()).isNotNull();
    } catch (BulkResponse.MalformedResponseException expected) {
      // the documented outcome for a body that is not a bulk response
    }
  }
}
