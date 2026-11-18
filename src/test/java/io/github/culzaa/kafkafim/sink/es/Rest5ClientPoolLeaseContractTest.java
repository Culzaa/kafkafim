package io.github.culzaa.kafkafim.sink.es;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.util.Timeout;
import org.junit.jupiter.api.Test;

/**
 * Pins what the low-level Elasticsearch client throws when no pooled connection can be leased in
 * time, as seen through {@link BulkTransport#of(Rest5Client)}: the exception the sink must
 * recognise (#398). The async HttpClient 5 under {@code Rest5Client} fails the lease with
 * HttpCore's {@code DeadlineTimeoutException} (wrapped in a {@code RuntimeException}, then an
 * {@code IOException}), not with the blocking client's {@code ConnectionRequestTimeoutException};
 * if a client upgrade changes the shape, this test says so before the circuit breaker starts
 * counting local pool exhaustion as cluster failures again. HttpClient 5 expires a waiting lease
 * lazily, when the pool next processes a release, so the server holds the only connection for two
 * seconds: long past the 300 ms lease timeout.
 */
class Rest5ClientPoolLeaseContractTest {

  @Test
  void leaseTimeoutOfTheSharedPoolIsRecognisedByTheSink() throws Exception {
    CountDownLatch firstRequestArrived = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    HttpServer server =
        HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext(
        "/_bulk",
        exchange -> {
          firstRequestArrived.countDown();
          try {
            release.await(2, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          byte[] body = "{\"errors\":false,\"items\":[]}".getBytes(UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
          }
        });
    server.start();
    try (Rest5Client client =
        Rest5Client.builder(
                new HttpHost(
                    "http",
                    server.getAddress().getAddress().getHostAddress(),
                    server.getAddress().getPort()))
            .setRequestConfigCallback(
                config -> config.setConnectionRequestTimeout(Timeout.ofMilliseconds(300)))
            .setConnectionManagerCallback(pool -> pool.setMaxConnPerRoute(1).setMaxConnTotal(1))
            .build()) {
      BulkTransport transport = BulkTransport.of(client);
      byte[] body = "{\"index\":{}}\n{}\n".getBytes(UTF_8);
      // The only pooled connection is held by a request the server does not answer.
      final CompletableFuture<Object> holder =
          CompletableFuture.supplyAsync(() -> {
            try {
              return transport.send(body);
            } catch (IOException e) {
              return e;
            }
          });
      assertThat(firstRequestArrived.await(10, TimeUnit.SECONDS)).isTrue();

      assertThatThrownBy(() -> transport.send(body))
          .isInstanceOf(IOException.class)
          .satisfies(e -> assertThat(BulkSink.isPoolLeaseTimeout(e)).isTrue());

      release.countDown();
      assertThat(holder.get(10, TimeUnit.SECONDS)).isInstanceOf(BulkTransport.Result.class);
    } finally {
      release.countDown();
      server.stop(0);
    }
  }
}
