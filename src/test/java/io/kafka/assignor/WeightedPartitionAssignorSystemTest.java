package io.kafka.assignor;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * System test that verifies weighted partition rebalancing against a real Kafka broker
 * using the {@link ThroughputWeightProvider} to sample live message rates.
 *
 * Setup: 4 consumers, 10 partitions, only 2 heavy (p0, p1) and 8 light (p2-p9).
 * A background producer writes ~100 msg/iteration to p0 and p1 (~1000 msg/s each)
 * and ~1 msg/iteration to p2-p9 (~10 msg/s each), creating a ~100:1 throughput ratio.
 *
 * With fewer heavy partitions than consumers, the rebalancer can't give each consumer
 * an equal share of heavy partitions. Instead, it sacrifices partition-count evenness to
 * balance weight: consumers holding a heavy partition get fewer total partitions, while
 * consumers without heavy partitions absorb more light ones.
 *
 * A standard count-based assignor would produce partition counts of 3,3,2,2.
 * The weighted rebalancer instead produces counts like 1,1,4,4.
 */
@Testcontainers
@Timeout(120)
class WeightedPartitionAssignorSystemTest {

    private static final String TOPIC = "weighted-test";
    private static final int NUM_PARTITIONS = 10;
    private static final int NUM_HEAVY_PARTITIONS = 2;
    private static final int NUM_CONSUMERS = 4;
    private static final String GROUP_ID = "weighted-system-test-group";

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:5.5.0"));

    private static final AtomicBoolean producerRunning = new AtomicBoolean(true);
    private static Thread producerThread;

    @BeforeAll
    static void setUp() throws ExecutionException, InterruptedException {
        createTopic();
        startBackgroundProducer();
        // Wait for messages to accumulate so ThroughputWeightProvider can sample real rate differences
        Thread.sleep(3000);
    }

    @AfterAll
    static void tearDown() throws InterruptedException {
        producerRunning.set(false);
        if (producerThread != null) {
            producerThread.join(5000);
        }
    }

    private static void createTopic() throws ExecutionException, InterruptedException {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (Admin admin = AdminClient.create(adminProps)) {
            admin.createTopics(Collections.singletonList(
                    new NewTopic(TOPIC, NUM_PARTITIONS, (short) 1))).all().get();
        }
    }

    private static void startBackgroundProducer() {
        producerThread = new Thread(() -> {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                while (producerRunning.get()) {
                    // High throughput: partitions 0-1 get ~100 messages per iteration (~1000 msg/s each)
                    for (int p = 0; p < NUM_HEAVY_PARTITIONS; p++) {
                        for (int i = 0; i < 100; i++) {
                            producer.send(new ProducerRecord<>(TOPIC, p, null, "msg"));
                        }
                    }
                    // Low throughput: partitions 2-9 get 1 message per iteration (~10 msg/s each)
                    for (int p = NUM_HEAVY_PARTITIONS; p < NUM_PARTITIONS; p++) {
                        producer.send(new ProducerRecord<>(TOPIC, p, null, "msg"));
                    }
                    producer.flush();
                    Thread.sleep(100);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "background-producer");
        producerThread.setDaemon(true);
        producerThread.start();
    }

    private KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                WeightedPartitionAssignor.class.getName());
        props.put(WeightedPartitionAssignor.WEIGHT_PROVIDER_CLASS_CONFIG,
                ThroughputWeightProvider.class.getName());
        props.put(ThroughputWeightProvider.SAMPLING_INTERVAL_MS_CONFIG, "2000");
        props.put(ThroughputWeightProvider.MIN_WEIGHT_CONFIG, "1.0");
        return new KafkaConsumer<>(props);
    }

    @Test
    void weightedRebalancerSacrificesCountEvennessForWeightBalance() throws InterruptedException {
        List<KafkaConsumer<String, String>> consumers = new ArrayList<>();
        for (int i = 0; i < NUM_CONSUMERS; i++) {
            consumers.add(createConsumer());
        }

        try {
            Map<String, Set<TopicPartition>> assignments = new HashMap<>();
            AtomicBoolean stable = new AtomicBoolean(false);

            List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < NUM_CONSUMERS; i++) {
                KafkaConsumer<String, String> consumer = consumers.get(i);
                consumer.subscribe(Collections.singletonList(TOPIC));
                threads.add(startConsumerThread(consumer, "consumer-" + i, assignments, stable));
            }

            // Wait until all consumers have non-empty assignments totalling NUM_PARTITIONS
            long deadline = System.currentTimeMillis() + 60_000;
            while (System.currentTimeMillis() < deadline) {
                synchronized (assignments) {
                    if (assignments.size() == NUM_CONSUMERS) {
                        int totalAssigned = 0;
                        boolean allNonEmpty = true;
                        for (Set<TopicPartition> a : assignments.values()) {
                            if (a.isEmpty()) {
                                allNonEmpty = false;
                                break;
                            }
                            totalAssigned += a.size();
                        }
                        if (allNonEmpty && totalAssigned == NUM_PARTITIONS) {
                            stable.set(true);
                            break;
                        }
                    }
                }
                Thread.sleep(500);
            }

            assertTrue(stable.get(),
                    "Consumers did not reach stable assignment within 60 seconds. Current: " + assignments);

            for (Thread t : threads) {
                t.join(5000);
            }

            // Classify consumers into heavy-holders (has p0 or p1) and light-only
            List<String> heavyConsumers = new ArrayList<>();
            List<String> lightOnlyConsumers = new ArrayList<>();

            for (Map.Entry<String, Set<TopicPartition>> entry : assignments.entrySet()) {
                boolean hasHeavy = false;
                for (TopicPartition tp : entry.getValue()) {
                    if (tp.partition() < NUM_HEAVY_PARTITIONS) {
                        hasHeavy = true;
                        break;
                    }
                }
                if (hasHeavy) {
                    heavyConsumers.add(entry.getKey());
                } else {
                    lightOnlyConsumers.add(entry.getKey());
                }
            }

            // Assertion 1: The 2 heavy partitions are on exactly 2 different consumers
            assertEquals(2, heavyConsumers.size(),
                    "Expected exactly 2 consumers holding heavy partitions, but got "
                            + heavyConsumers.size() + ". Assignment: " + assignments);

            // Assertion 2: No consumer holds both heavy partitions
            for (String name : heavyConsumers) {
                int heavyCount = 0;
                for (TopicPartition tp : assignments.get(name)) {
                    if (tp.partition() < NUM_HEAVY_PARTITIONS) {
                        heavyCount++;
                    }
                }
                assertEquals(1, heavyCount,
                        "Consumer " + name + " holds " + heavyCount
                                + " heavy partitions (expected 1). Assignment: " + assignments);
            }

            // Assertion 3: Heavy consumers have FEWER partitions than light-only consumers.
            // This proves the rebalancer sacrificed partition-count evenness for weight balance.
            // A count-based assignor would give 2 or 3 partitions to each (10/4 = 2.5).
            // The weighted rebalancer gives ~1 to heavy consumers, ~4 to light-only consumers.
            int maxHeavyConsumerSize = 0;
            int minLightConsumerSize = Integer.MAX_VALUE;
            for (String name : heavyConsumers) {
                maxHeavyConsumerSize = Math.max(maxHeavyConsumerSize, assignments.get(name).size());
            }
            for (String name : lightOnlyConsumers) {
                minLightConsumerSize = Math.min(minLightConsumerSize, assignments.get(name).size());
            }
            assertTrue(maxHeavyConsumerSize < minLightConsumerSize,
                    "Heavy consumers should have strictly fewer partitions than light-only consumers. "
                            + "Heavy consumer max size: " + maxHeavyConsumerSize
                            + ", light-only consumer min size: " + minLightConsumerSize
                            + ". Assignment: " + assignments);

            // Assertion 4: Partition counts are clearly uneven
            assertTrue(maxHeavyConsumerSize <= 2,
                    "Heavy consumers should have at most 2 partitions, got " + maxHeavyConsumerSize
                            + ". Assignment: " + assignments);
            assertTrue(minLightConsumerSize >= 3,
                    "Light-only consumers should have at least 3 partitions, got " + minLightConsumerSize
                            + ". Assignment: " + assignments);

        } finally {
            for (KafkaConsumer<String, String> consumer : consumers) {
                consumer.close(Duration.ofSeconds(5));
            }
        }
    }

    private Thread startConsumerThread(KafkaConsumer<String, String> consumer, String name,
                                       Map<String, Set<TopicPartition>> assignments,
                                       AtomicBoolean stable) {
        Thread t = new Thread(() -> {
            while (!stable.get()) {
                consumer.poll(Duration.ofMillis(1000));
                Set<TopicPartition> assigned = consumer.assignment();
                if (!assigned.isEmpty()) {
                    synchronized (assignments) {
                        assignments.put(name, assigned);
                    }
                }
            }
        }, name);
        t.setDaemon(true);
        t.start();
        return t;
    }
}
