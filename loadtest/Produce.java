import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Load generator, run on the host (java -cp 'kafka-clients/*' Produce.java ...).
 * Arguments: bootstrap topic firstSeq count ratePerSecond(0 = unthrottled) docBytes.
 * Documents are JSON objects with a unique id (the sequence number), so every document is a
 * new Elasticsearch document and a re-delivery overwrites instead of adding. The Kafka record
 * timestamp is the send time (CreateTime), which is what kafkafim_e2e_latency_seconds measures.
 */
public final class Produce {
  public static void main(String[] a) throws Exception {
    String topic = a[1];
    long first = Long.parseLong(a[2]);
    long count = Long.parseLong(a[3]);
    long rate = Long.parseLong(a[4]);
    int docBytes = Integer.parseInt(a[5]);
    Properties p = new Properties();
    p.put("bootstrap.servers", a[0]);
    p.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    p.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    p.put("linger.ms", "5");
    p.put("compression.type", "lz4");
    p.put("acks", "1");
    StringBuilder pad = new StringBuilder();
    for (int i = 0; pad.length() < docBytes; i++) {
      pad.append("lorem ipsum dolor sit amet ").append(i % 97).append(' ');
    }
    long start = System.nanoTime();
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(p)) {
      for (long i = 0; i < count; i++) {
        if (rate > 0) {
          long due = start + i * 1_000_000_000L / rate;
          long wait = due - System.nanoTime();
          if (wait > 0) {
            Thread.sleep(wait / 1_000_000, (int) (wait % 1_000_000));
          }
        }
        long id = first + i;
        String json =
            "{\"id\":\"" + id + "\",\"region\":\"eu\",\"seq\":" + id + ",\"msg\":\""
                + pad.substring(0, Math.max(0, docBytes - 60)) + "\"}";
        producer.send(new ProducerRecord<>(topic, json.getBytes(StandardCharsets.UTF_8)));
      }
      producer.flush();
    }
    double s = (System.nanoTime() - start) / 1e9;
    System.out.printf("produced %d records to %s in %.1fs (%.0f/s)%n", count, topic, s, count / s);
  }
}
