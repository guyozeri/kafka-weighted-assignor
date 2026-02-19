import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import io.kafka.assignor.ThroughputWeightProvider;
import io.kafka.assignor.WeightedPartitionAssignor;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Standalone example showing how to wire the weighted partition assignor
 * into a KafkaConsumer.
 *
 * This file lives outside src/ and is NOT compiled into the library jar.
 * Copy it into your own project and adjust the configuration to suit your needs.
 */
public class WeightedConsumerExample {

    public static void main(String[] args) {
        Properties props = new Properties();

        // ---- Standard Kafka consumer configs ----
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-weighted-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // ---- Weighted assignor configs ----
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                WeightedPartitionAssignor.class.getName());

        // Use ThroughputWeightProvider to weight partitions by their message rate.
        // It samples offsets via the Admin API to estimate per-partition throughput.
        props.put(WeightedPartitionAssignor.WEIGHT_PROVIDER_CLASS_CONFIG,
                ThroughputWeightProvider.class.getName());

        // How long the provider waits between the two offset samples (default 2000ms)
        props.put(ThroughputWeightProvider.SAMPLING_INTERVAL_MS_CONFIG, "3000");

        // Floor weight for partitions with zero or negligible throughput (default 1.0)
        props.put(ThroughputWeightProvider.MIN_WEIGHT_CONFIG, "1.0");

        // ---- Alternative: UniformWeightProvider (the default) ----
        // If you omit the weight provider class, all partitions receive equal weight
        // and the assignor behaves like a standard sticky assignor.
        //
        // props.remove(WeightedPartitionAssignor.WEIGHT_PROVIDER_CLASS_CONFIG);

        // ---- Create consumer, subscribe, and poll ----
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("my-topic"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("partition=%d offset=%d key=%s value=%s%n",
                            record.partition(), record.offset(), record.key(), record.value());
                }
            }
        }
    }
}
