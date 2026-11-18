package io.github.matabar.kafkafim.dlq;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;
import org.jspecify.annotations.Nullable;

/**
 * The DLQ producer of one {@code (cluster, identity)}, shared by every pipeline's {@link DlqWriter}
 * on it, plus which DLQ topics it has seen exist.
 *
 * <p>Before the first record to a topic, the topic is described: kafka-clients' producer always
 * asks the broker to auto-create a topic it sends to, so producing blind would create a missing DLQ
 * topic on a broker with {@code auto.create.topics.enable=true} (ADR-0021 forbids it), and would
 * otherwise only surface as a metadata timeout. A missing, forbidden or unreachable topic is
 * remembered for one second, so that many sends blocked on it do not describe it once each.
 *
 * <p>The describe is single-flight per topic: concurrent sends that find the cache cold or its
 * negative entry expired wait for one describe instead of firing one each.
 */
final class SharedProducer {

  private static final long NEGATIVE_TTL_NANOS = Duration.ofSeconds(1).toNanos();

  private final String cluster;
  private final @Nullable String identity;
  private final DlqClient client;
  private final Map<String, Checked> topics = new ConcurrentHashMap<>();
  private final Map<String, CompletableFuture<@Nullable TopicStatus>> inflight =
      new ConcurrentHashMap<>();
  private final LongSupplier nanoTime;

  SharedProducer(String cluster, @Nullable String identity, DlqClient client) {
    this(cluster, identity, client, System::nanoTime);
  }

  SharedProducer(
      String cluster, @Nullable String identity, DlqClient client, LongSupplier nanoTime) {
    this.cluster = cluster;
    this.identity = identity;
    this.client = client;
    this.nanoTime = nanoTime;
  }

  String cluster() {
    return cluster;
  }

  @Nullable String identity() {
    return identity;
  }

  DlqClient client() {
    return client;
  }

  /**
   * The topic's status, describing it unless it is known to exist or was just found not to. Of
   * concurrent callers needing a describe, one (the leader) describes and the others wait for its
   * answer. If the leader was interrupted its answer is no answer: it is neither cached nor handed
   * to the waiters, which then describe themselves. If the leader's describe throws, the waiters
   * rethrow its exception. A waiter that waits longer than the timeout plus one second gets {@link
   * TopicStatus#UNREACHABLE}, uncached. A waiter interrupted while waiting gets {@link
   * TopicStatus#UNREACHABLE} with its interrupt flag set, uncached, as {@link DlqClient#describe}
   * does.
   */
  TopicStatus ensureTopic(String topic, Duration timeout) {
    while (true) {
      Checked last = topics.get(topic);
      if (last != null
          && (last.status == TopicStatus.EXISTS
              || nanoTime.getAsLong() - last.atNanos < NEGATIVE_TTL_NANOS)) {
        return last.status;
      }
      CompletableFuture<@Nullable TopicStatus> mine = new CompletableFuture<>();
      CompletableFuture<@Nullable TopicStatus> leader = inflight.putIfAbsent(topic, mine);
      if (leader == null) {
        return lead(topic, timeout, mine);
      }
      try {
        TopicStatus shared =
            leader.get(timeout.toNanos() + NEGATIVE_TTL_NANOS, TimeUnit.NANOSECONDS);
        if (shared != null) {
          return shared;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return TopicStatus.UNREACHABLE;
      } catch (ExecutionException e) {
        if (e.getCause() instanceof RuntimeException failure) {
          throw failure;
        }
        throw new IllegalStateException(e.getCause());
      } catch (TimeoutException e) {
        return TopicStatus.UNREACHABLE;
      }
    }
  }

  private TopicStatus lead(
      String topic, Duration timeout, CompletableFuture<@Nullable TopicStatus> mine) {
    TopicStatus status = null;
    TopicStatus answer = null;
    try {
      status = client.describe(topic, timeout);
      answer = remember(topic, status) ? status : null;
      return status;
    } catch (RuntimeException e) {
      mine.completeExceptionally(e);
      throw e;
    } finally {
      inflight.remove(topic, mine);
      mine.complete(answer);
    }
  }

  /**
   * Describes the topic now and remembers the result, unless the calling thread was interrupted
   * (see {@link #remember}). Always asks the broker, whatever is cached or in flight.
   */
  TopicStatus check(String topic, Duration timeout) {
    TopicStatus status = client.describe(topic, timeout);
    remember(topic, status);
    return status;
  }

  /**
   * Caches the status, unless the calling thread was interrupted: {@link DlqClient#describe} then
   * answers {@link TopicStatus#UNREACHABLE} without any broker saying so, and other pipelines'
   * sends on this shared producer must not see that for the negative TTL.
   *
   * @return whether it was cached
   */
  private boolean remember(String topic, TopicStatus status) {
    if (status == TopicStatus.UNREACHABLE && Thread.currentThread().isInterrupted()) {
      return false;
    }
    topics.put(topic, new Checked(status, nanoTime.getAsLong()));
    return true;
  }

  /** Forgets that the topic exists, so that the next send checks again (it may be deleted). */
  void forget(String topic) {
    topics.remove(topic);
  }

  /** {@code identity 'x' on Kafka cluster 'y'}, for messages. */
  String describeIdentity() {
    return (identity == null ? "no identity" : "identity '" + identity + "'")
        + " on Kafka cluster '"
        + cluster
        + "'";
  }

  private record Checked(TopicStatus status, long atNanos) {}
}
