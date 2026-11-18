package io.github.matabar.kafkafim.dlq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class SharedProducerTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(10);
  private static final String TOPIC = "orders.kafkafim-dlq";

  private final AtomicLong clock = new AtomicLong();
  private final BlockingClient client = new BlockingClient();
  private final SharedProducer producer = new SharedProducer("local", null, client, clock::get);
  private final ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();

  @AfterEach
  void tearDown() {
    client.release.countDown();
    pool.shutdownNow();
  }

  @Test
  void coldCacheConcurrentSendsShareOneDescribe() throws Exception {
    final List<Future<TopicStatus>> calls = start(8);
    await().atMost(TIMEOUT).until(() -> client.describes.get() >= 1);
    // Give the other callers time to (wrongly) describe too.
    await().during(Duration.ofMillis(200)).until(() -> client.describes.get() == 1);

    client.release.countDown();

    for (Future<TopicStatus> call : calls) {
      assertThat(call.get(10, TimeUnit.SECONDS)).isEqualTo(TopicStatus.EXISTS);
    }
    assertThat(client.describes.get()).isEqualTo(1);
  }

  @Test
  void expiredNegativeEntryConcurrentSendsShareOneDescribe() throws Exception {
    client.release.countDown();
    client.status.set(TopicStatus.MISSING);
    assertThat(producer.ensureTopic(TOPIC, TIMEOUT)).isEqualTo(TopicStatus.MISSING);
    assertThat(producer.ensureTopic(TOPIC, TIMEOUT))
        .as("still remembered")
        .isEqualTo(TopicStatus.MISSING);
    assertThat(client.describes.get()).isEqualTo(1);

    clock.addAndGet(Duration.ofSeconds(2).toNanos());
    client.release = new CountDownLatch(1);
    final List<Future<TopicStatus>> calls = start(8);
    await().atMost(TIMEOUT).until(() -> client.describes.get() >= 2);
    await().during(Duration.ofMillis(200)).until(() -> client.describes.get() == 2);
    client.release.countDown();

    for (Future<TopicStatus> call : calls) {
      assertThat(call.get(10, TimeUnit.SECONDS)).isEqualTo(TopicStatus.MISSING);
    }
    assertThat(client.describes.get()).isEqualTo(2);
  }

  @Test
  void knownTopicIsNotDescribedAgainUntilForgotten() {
    client.release.countDown();

    producer.ensureTopic(TOPIC, TIMEOUT);
    producer.ensureTopic(TOPIC, TIMEOUT);
    assertThat(client.describes.get()).isEqualTo(1);

    producer.forget(TOPIC);
    producer.ensureTopic(TOPIC, TIMEOUT);

    assertThat(client.describes.get()).isEqualTo(2);
  }

  @Test
  void interruptedLeaderCachesNothingAndWaitersDescribeThemselves() throws Exception {
    AtomicReference<Thread> leaderThread = new AtomicReference<>();
    final Future<TopicStatus> leader =
        pool.submit(() -> {
          leaderThread.set(Thread.currentThread());
          return producer.ensureTopic(TOPIC, TIMEOUT);
        });
    await().atMost(TIMEOUT).until(() -> client.describes.get() == 1);
    final Future<TopicStatus> waiter = pool.submit(() -> producer.ensureTopic(TOPIC, TIMEOUT));
    await().during(Duration.ofMillis(200)).until(() -> client.describes.get() == 1);

    leaderThread.get().interrupt();

    assertThat(leader.get(10, TimeUnit.SECONDS)).isEqualTo(TopicStatus.UNREACHABLE);
    // The waiter did not take the leader's non-answer: it described itself (and was released).
    await().atMost(TIMEOUT).until(() -> client.describes.get() == 2);
    client.release.countDown();
    assertThat(waiter.get(10, TimeUnit.SECONDS)).isEqualTo(TopicStatus.EXISTS);
    assertThat(producer.ensureTopic(TOPIC, TIMEOUT)).isEqualTo(TopicStatus.EXISTS);
    assertThat(client.describes.get()).as("the waiter's answer was cached").isEqualTo(2);
  }

  @Test
  void interruptedWaiterGetsUnreachableUncachedWhileLeaderCaches() throws Exception {
    final Future<TopicStatus> leader = pool.submit(() -> producer.ensureTopic(TOPIC, TIMEOUT));
    await().atMost(TIMEOUT).until(() -> client.describes.get() == 1);
    AtomicReference<Thread> waiterThread = new AtomicReference<>();
    AtomicReference<Boolean> flag = new AtomicReference<>();
    final Future<TopicStatus> waiter =
        pool.submit(() -> {
          waiterThread.set(Thread.currentThread());
          TopicStatus status = producer.ensureTopic(TOPIC, TIMEOUT);
          flag.set(Thread.currentThread().isInterrupted());
          return status;
        });
    await().atMost(TIMEOUT).until(() -> waiterThread.get() != null);
    await().during(Duration.ofMillis(200)).until(() -> !waiter.isDone());

    waiterThread.get().interrupt();

    assertThat(waiter.get(10, TimeUnit.SECONDS)).isEqualTo(TopicStatus.UNREACHABLE);
    assertThat(flag.get()).as("interrupt flag restored").isTrue();
    client.release.countDown();
    assertThat(leader.get(10, TimeUnit.SECONDS)).isEqualTo(TopicStatus.EXISTS);
    assertThat(producer.ensureTopic(TOPIC, TIMEOUT)).isEqualTo(TopicStatus.EXISTS);
    assertThat(client.describes.get()).isEqualTo(1);
  }

  @Test
  void failingDescribeFailsTheLeaderAndLeavesNothingInFlight() throws Exception {
    client.failure.set(new KafkaException("admin broken"));
    client.release.countDown();

    assertThatThrownBy(() -> producer.ensureTopic(TOPIC, TIMEOUT))
        .isInstanceOf(KafkaException.class);

    assertThat(producer.ensureTopic(TOPIC, TIMEOUT)).isEqualTo(TopicStatus.EXISTS);
    assertThat(client.describes.get()).isEqualTo(2);
  }

  private List<Future<TopicStatus>> start(int callers) {
    final List<Future<TopicStatus>> calls = new ArrayList<>();
    for (int i = 0; i < callers; i++) {
      calls.add(pool.submit(() -> producer.ensureTopic(TOPIC, TIMEOUT)));
    }
    return calls;
  }

  /** Describes block until the test releases them. */
  private static final class BlockingClient implements DlqClient {
    final AtomicInteger describes = new AtomicInteger();
    final AtomicReference<TopicStatus> status = new AtomicReference<>(TopicStatus.EXISTS);
    final AtomicReference<RuntimeException> failure = new AtomicReference<>();
    volatile CountDownLatch release = new CountDownLatch(1);

    @Override
    public void send(ProducerRecord<byte[], byte[]> record, Callback callback) {
      throw new UnsupportedOperationException();
    }

    @Override
    public TopicStatus describe(String topic, Duration timeout) {
      describes.incrementAndGet();
      RuntimeException f = failure.getAndSet(null);
      if (f != null) {
        throw f;
      }
      try {
        release.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return TopicStatus.UNREACHABLE;
      }
      return Thread.currentThread().isInterrupted() ? TopicStatus.UNREACHABLE : status.get();
    }

    @Override
    public void close(Duration timeout) {}
  }
}
