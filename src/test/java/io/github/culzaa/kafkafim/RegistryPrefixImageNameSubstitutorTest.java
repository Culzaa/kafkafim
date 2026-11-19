package io.github.culzaa.kafkafim;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;

class RegistryPrefixImageNameSubstitutorTest {

  private static final String PREFIX = "registry.example:5000/mirror";

  private static String apply(String prefix, String image) {
    return new RegistryPrefixImageNameSubstitutor(prefix)
        .apply(DockerImageName.parse(image))
        .asCanonicalNameString();
  }

  @Test
  void prefixesDockerHubImages() {
    assertThat(apply(PREFIX, "apache/kafka:4.3.1")).isEqualTo(PREFIX + "/apache/kafka:4.3.1");
    assertThat(apply(PREFIX, "testcontainers/ryuk:0.14.0"))
        .isEqualTo(PREFIX + "/testcontainers/ryuk:0.14.0");
  }

  @Test
  void prefixesImagesFromOtherRegistries() {
    assertThat(apply(PREFIX, "docker.elastic.co/elasticsearch/elasticsearch:9.5.6"))
        .isEqualTo(PREFIX + "/docker.elastic.co/elasticsearch/elasticsearch:9.5.6");
    assertThat(apply(PREFIX, "ghcr.io/example/tool:1.0"))
        .isEqualTo(PREFIX + "/ghcr.io/example/tool:1.0");
  }

  @Test
  void keepsAnImageThatAlreadyCarriesThePrefix() {
    String once = PREFIX + "/apache/kafka:4.3.1";
    assertThat(apply(PREFIX, once)).isEqualTo(once);
  }

  @Test
  void ignoresTrailingSlashAndSurroundingSpace() {
    assertThat(apply(" " + PREFIX + "/ ", "apache/kafka:4.3.1"))
        .isEqualTo(PREFIX + "/apache/kafka:4.3.1");
  }

  @Test
  void changesNothingWithoutPrefix() {
    assertThat(apply("", "docker.elastic.co/elasticsearch/elasticsearch:9.5.6"))
        .isEqualTo("docker.elastic.co/elasticsearch/elasticsearch:9.5.6");
    assertThat(apply(null, "apache/kafka:4.3.1")).isEqualTo("apache/kafka:4.3.1");
  }

  @Test
  void theSubstituteIsCompatibleWithTheOriginal() {
    DockerImageName original = DockerImageName.parse("apache/kafka:4.3.1");
    DockerImageName substitute = new RegistryPrefixImageNameSubstitutor(PREFIX).apply(original);
    // Container classes check this before they accept an image (KafkaContainer, for one).
    substitute.assertCompatibleWith(original);
  }
}
