package io.github.culzaa.kafkafim;

import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.ImageNameSubstitutor;

/**
 * Pulls every Testcontainers image through a registry prefix, for CI where Docker Hub, {@code
 * docker.elastic.co} and {@code ghcr.io} are unreachable (docs/DELIVERY.md, "GitLab CI").
 *
 * <p>Testcontainers' own {@code TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX} skips an image whose name
 * carries a registry ("is not a Docker Hub image"), so it would leave the Elasticsearch image on
 * {@code docker.elastic.co}. This substitutor prefixes every image: {@code apache/kafka:4.3.1}
 * becomes {@code <prefix>/apache/kafka:4.3.1} and {@code
 * docker.elastic.co/elasticsearch/elasticsearch:9.5.6} becomes {@code
 * <prefix>/docker.elastic.co/elasticsearch/elasticsearch:9.5.6}, the names that {@code
 * tools/airgap-dev-bundle.sh publish} pushes. The prefix is the environment variable {@value
 * #PREFIX_VARIABLE}; when it is empty or unset nothing changes. It is activated by {@code
 * TESTCONTAINERS_IMAGE_SUBSTITUTOR}, so a build without that variable never loads this class.
 */
public final class RegistryPrefixImageNameSubstitutor extends ImageNameSubstitutor {

  /** The environment variable that holds the registry prefix. */
  static final String PREFIX_VARIABLE = "KAFKAFIM_IMAGE_PREFIX";

  private final String prefix;

  /** Reads the prefix from the environment; Testcontainers instantiates this class that way. */
  public RegistryPrefixImageNameSubstitutor() {
    this(System.getenv(PREFIX_VARIABLE));
  }

  RegistryPrefixImageNameSubstitutor(String prefix) {
    this.prefix = normalize(prefix);
  }

  @Override
  public DockerImageName apply(DockerImageName original) {
    if (prefix.isEmpty() || original.asCanonicalNameString().startsWith(prefix + "/")) {
      return original;
    }
    return DockerImageName.parse(prefix + "/" + original.asCanonicalNameString())
        .asCompatibleSubstituteFor(original);
  }

  @Override
  protected String getDescription() {
    return "kafkafim registry prefix '" + prefix + "'";
  }

  private static String normalize(String value) {
    String trimmed = value == null ? "" : value.trim();
    while (trimmed.endsWith("/")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    return trimmed;
  }
}
