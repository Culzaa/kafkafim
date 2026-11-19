package io.github.culzaa.kafkafim;

import com.github.dockerjava.api.command.CreateContainerCmd;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import org.testcontainers.DockerClientFactory;

/**
 * Makes a TLS test container's certificate match the name the test connects to when Docker runs
 * elsewhere.
 *
 * <p>Elasticsearch generates its HTTPS certificate for the container's own names. With a local
 * Docker daemon the tests reach it at {@code localhost}, which is in the certificate. With
 * Docker-in-Docker (a GitLab job: {@code TESTCONTAINERS_HOST_OVERRIDE=docker}) Testcontainers
 * reaches it at {@code docker}, which is not, and hostname verification fails. Naming the container
 * after that host puts the name in the certificate. With a local daemon this does nothing.
 */
public final class DockerHostNames {

  private static final Pattern DNS_NAME = Pattern.compile("[A-Za-z][A-Za-z0-9.-]*");

  private DockerHostNames() {}

  /**
   * A container modifier for {@code withCreateContainerCmdModifier}: sets the container's host name
   * to the Docker host's name unless that is {@code localhost} or an address.
   */
  public static Consumer<CreateContainerCmd> reachableAsDockerHost() {
    return cmd -> {
      String host = DockerClientFactory.instance().dockerHostIpAddress();
      if (!"localhost".equals(host) && DNS_NAME.matcher(host).matches()) {
        cmd.withHostName(host);
      }
    };
  }
}
