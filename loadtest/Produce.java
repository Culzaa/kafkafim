import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Load generator, run on the host (java -cp 'kafka-clients/*' Produce.java ...).
 * Arguments: bootstrap topic firstSeq count ratePerSecond(0 = unthrottled) docBytes.
 * Documents are JSON objects with a unique id (the sequence number), so every document is a
 * new Elasticsearch document and a re-delivery overwrites instead of adding. The Kafka record
 * timestamp is the send time (CreateTime), which is what kafkafim.e2e.latency measures.
 */
public final class Produce {
  private static final int ARGUMENT_COUNT = 6;
  private static final long NANOS_PER_SECOND = 1_000_000_000L;
  private static final long NANOS_PER_MILLI = 1_000_000L;

  private Produce() {}

  /**
   * Produces {@code count} documents to a topic, then prints a one-line summary.
   *
   * @param args bootstrap topic firstSeq count ratePerSecond docBytes (see the class comment)
   * @throws Exception when the producer or the throttling sleep fails
   */
  public static void main(String[] args) throws Exception {
    if (args.length != ARGUMENT_COUNT) {
      usage("expected " + ARGUMENT_COUNT + " arguments, got " + args.length);
    }
    String bootstrapServers = args[0];
    String topic = args[1];
    long firstSeq;
    long count;
    long ratePerSecond;
    int docBytes;
    try {
      firstSeq = Long.parseLong(args[2]);
      count = Long.parseLong(args[3]);
      ratePerSecond = Long.parseLong(args[4]);
      docBytes = Integer.parseInt(args[5]);
    } catch (NumberFormatException e) {
      usage("not a number: " + e.getMessage());
      return;
    }
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerConfig.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerConfig.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerConfig.put(ProducerConfig.LINGER_MS_CONFIG, "5");
    producerConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "1");
    StringBuilder pad = new StringBuilder();
    for (int i = 0; pad.length() < docBytes; i++) {
      pad.append("lorem ipsum dolor sit amet ").append(i % 97).append(' ');
    }
    long start = System.nanoTime();
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerConfig)) {
      for (long i = 0; i < count; i++) {
        if (ratePerSecond > 0) {
          long due = start + i * NANOS_PER_SECOND / ratePerSecond;
          long wait = due - System.nanoTime();
          if (wait > 0) {
            Thread.sleep(wait / NANOS_PER_MILLI, (int) (wait % NANOS_PER_MILLI));
          }
        }
        long id = firstSeq + i;
        String json =
            "{\"id\":\"" + id + "\",\"region\":\"eu\",\"seq\":" + id + ",\"msg\":\""
                + pad.substring(0, Math.max(0, docBytes - 60)) + "\"}";
        producer.send(new ProducerRecord<>(topic, json.getBytes(StandardCharsets.UTF_8)));
      }
      producer.flush();
    }
    double elapsedSeconds = (System.nanoTime() - start) / (double) NANOS_PER_SECOND;
    System.out.printf(
        "produced %d records to %s in %.1fs (%.0f/s)%n",
        count, topic, elapsedSeconds, count / elapsedSeconds);
  }

  private static void usage(String problem) {
    System.err.println(problem);
    System.err.println(
        "usage: Produce <bootstrap> <topic> <firstSeq> <count> <ratePerSecond> <docBytes>");
    System.exit(2);
  }
}
