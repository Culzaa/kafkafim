package io.github.matabar.kafkafim.source.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.matabar.kafkafim.config.Pipeline;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.util.unit.DataSize;

/**
 * The rebalance callbacks, the commit helpers and the commit-interval gating of {@link
 * KafkaPipelineSource}, on a fake consumer and a manual clock: no broker, no sleeps.
 */
class KafkaPipelineSourceCommitTest {

  private static final TopicPartition P0 = new TopicPartition("t", 0);
  private static final Duration INTERVAL = Duration.ofSeconds(1);

  private final AtomicLong clock = new AtomicLong();
  private final Handler handler = new Handler();
  private final FakeConsumer consumer = new FakeConsumer();
  private KafkaPipelineSource source;
  private KafkaPipelineSource.Rebalance rebalance;
  private KafkaPipelineSource.CommitBeforePoll beforePoll;

  @BeforeEach
  void setUp() {
    Pipeline.Source config =
        new Pipeline.Source(
            "local",
            List.of("t"),
            "g",
            null,
            Pipeline.OffsetReset.EARLIEST,
            500,
            DataSize.ofMegabytes(1),
            INTERVAL,
            DataSize.ofKilobytes(1),
            null);
    source =
        new KafkaPipelineSource(
            "p",
            config,
            new DefaultKafkaConsumerFactory<>(Map.of()),
            handler,
            SourceOptions.defaults(),
            clock::get);
    rebalance = source.new Rebalance();
    beforePoll = source.new CommitBeforePoll();
    consumer.assign(List.of(P0));
    rebalance.onPartitionsAssigned(consumer, List.of(P0));
  }

  private List<SourceRecord> deliver(int count) {
    List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      records.add(new ConsumerRecord<>("t", 0, i, null, new byte[10]));
    }
    source.new Listener().onMessage(records, consumer);
    return handler.records;
  }

  private void tick(Duration elapsed) {
    clock.addAndGet(elapsed.toNanos());
  }

  @Test
  void commitIsGatedByTheInterval() {
    final List<SourceRecord> records = deliver(3);
    beforePoll.setupThreadState(consumer);
    assertThat(consumer.asyncCommits).as("first call is due at once").hasSize(1);
    assertThat(consumer.asyncCommits.get(0).get(P0).offset()).isZero();
    consumer.asyncCommits.clear();

    records.get(0).complete();
    tick(INTERVAL.minusMillis(1));
    beforePoll.setupThreadState(consumer);
    assertThat(consumer.asyncCommits).as("interval not elapsed").isEmpty();

    tick(Duration.ofMillis(1));
    beforePoll.setupThreadState(consumer);
    assertThat(consumer.asyncCommits).hasSize(1);
    assertThat(consumer.asyncCommits.get(0).get(P0).offset()).isEqualTo(1L);
  }

  @Test
  void firstCallCommitsWithoutWaiting() {
    deliver(2).forEach(SourceRecord::complete);

    beforePoll.setupThreadState(consumer);

    assertThat(consumer.asyncCommits).hasSize(1);
    assertThat(consumer.asyncCommits.get(0).get(P0).offset()).isEqualTo(2L);
  }

  @Test
  void committedOffsetsAreNotCommittedAgain() {
    deliver(2).forEach(SourceRecord::complete);
    beforePoll.setupThreadState(consumer);
    tick(INTERVAL);
    beforePoll.setupThreadState(consumer);

    assertThat(consumer.asyncCommits).as("nothing advanced since").hasSize(1);
  }

  @Test
  void failedAsyncCommitIsRetriedNextIntervalWithTheSameOffsets() {
    deliver(2).forEach(SourceRecord::complete);
    consumer.asyncFailure = new KafkaException("broker unavailable");
    beforePoll.setupThreadState(consumer);
    assertThat(consumer.asyncCommits).hasSize(1);

    consumer.asyncFailure = null;
    tick(INTERVAL);
    beforePoll.setupThreadState(consumer);

    assertThat(consumer.asyncCommits).hasSize(2);
    assertThat(consumer.asyncCommits.get(1)).isEqualTo(consumer.asyncCommits.get(0));
    tick(INTERVAL);
    beforePoll.setupThreadState(consumer);
    assertThat(consumer.asyncCommits).as("success marks it committed").hasSize(2);
  }

  @Test
  void revokeCommitsWhatCompletedAndAbandonsTheRest() {
    List<SourceRecord> records = deliver(4);
    records.get(0).complete();
    records.get(1).complete();

    rebalance.onPartitionsRevokedBeforeCommit(consumer, List.of(P0));

    assertThat(handler.revoked).containsExactly(P0);
    assertThat(consumer.syncCommits).hasSize(1);
    assertThat(consumer.syncCommits.get(0).get(P0).offset()).isEqualTo(2L);
    assertThat(source.inFlightRecords()).isZero();
    assertThat(source.inFlightBytes()).isZero();
  }

  @Test
  void revokeStillAbandonsWhenTheSyncCommitFails() {
    deliver(2).forEach(SourceRecord::complete);
    consumer.syncFailure = new KafkaException("commit timed out");

    rebalance.onPartitionsRevokedBeforeCommit(consumer, List.of(P0));

    assertThat(handler.revoked).containsExactly(P0);
    assertThat(consumer.syncCommits).hasSize(1);
    assertThat(source.inFlightRecords()).isZero();
  }

  @Test
  void revokeCommitsEvenIfTheRevokeHandlerThrows() {
    deliver(2).forEach(SourceRecord::complete);
    handler.failures = true;

    rebalance.onPartitionsRevokedBeforeCommit(consumer, List.of(P0));

    assertThat(consumer.syncCommits).hasSize(1);
    assertThat(source.inFlightRecords()).isZero();
  }

  @Test
  void revokeWithNothingCompletedCommitsTheStartOffset() {
    deliver(2);

    rebalance.onPartitionsRevokedBeforeCommit(consumer, List.of(P0));

    assertThat(consumer.syncCommits).hasSize(1);
    assertThat(consumer.syncCommits.get(0).get(P0).offset()).isZero();
    assertThat(source.inFlightRecords()).isZero();
  }

  @Test
  void revokeOfUntrackedPartitionsIsIgnored() {
    rebalance.onPartitionsRevokedBeforeCommit(consumer, List.of(new TopicPartition("t", 9)));

    assertThat(handler.revoked).isEmpty();
    assertThat(consumer.syncCommits).isEmpty();
  }

  @Test
  void lostPartitionsAreNeverCommittedAndTheirRecordsAreAbandoned() {
    List<SourceRecord> records = deliver(3);
    records.get(0).complete();
    records.get(1).complete();

    rebalance.onPartitionsLost(consumer, List.of(P0));

    assertThat(handler.lost).containsExactly(P0);
    assertThat(handler.revoked).isEmpty();
    assertThat(consumer.syncCommits).isEmpty();
    assertThat(source.inFlightRecords()).isZero();
    assertThat(source.inFlightBytes()).isZero();

    records.get(2).complete();
    tick(INTERVAL);
    beforePoll.setupThreadState(consumer);
    assertThat(consumer.asyncCommits).as("late completions of lost records").isEmpty();
  }

  @Test
  void lostPartitionsAreAbandonedEvenIfTheLostHandlerThrows() {
    deliver(2);
    handler.failures = true;

    rebalance.onPartitionsLost(consumer, List.of(P0));

    assertThat(source.inFlightRecords()).isZero();
    assertThat(consumer.syncCommits).isEmpty();
  }

  @Test
  void lostOfUntrackedPartitionsIsIgnored() {
    rebalance.onPartitionsLost(consumer, List.of(new TopicPartition("t", 9)));

    assertThat(handler.lost).isEmpty();
  }

  private static final class Handler implements SourceRecordHandler {
    final List<SourceRecord> records = new ArrayList<>();
    final List<TopicPartition> revoked = new ArrayList<>();
    final List<TopicPartition> lost = new ArrayList<>();
    boolean failures;

    @Override
    public void onBatch(List<SourceRecord> batch) {
      records.addAll(batch);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      revoked.addAll(partitions);
      if (failures) {
        throw new IllegalStateException("revoke handler");
      }
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
      lost.addAll(partitions);
      if (failures) {
        throw new IllegalStateException("lost handler");
      }
    }
  }

  /** Records commits and fails them on demand. */
  private static final class FakeConsumer extends MockConsumer<byte[], byte[]> {
    final List<Map<TopicPartition, OffsetAndMetadata>> syncCommits = new ArrayList<>();
    final List<Map<TopicPartition, OffsetAndMetadata>> asyncCommits = new ArrayList<>();
    volatile KafkaException syncFailure;
    volatile Exception asyncFailure;

    FakeConsumer() {
      super("earliest");
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
      syncCommits.add(Map.copyOf(offsets));
      if (syncFailure != null) {
        throw syncFailure;
      }
    }

    @Override
    public void commitAsync(
        Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
      asyncCommits.add(Map.copyOf(offsets));
      callback.onComplete(offsets, asyncFailure);
    }
  }
}
