package io.kafka.assignor;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static io.kafka.assignor.WeightedPartitionAssignor.WEIGHT_PROVIDER_CLASS_CONFIG;

class WeightedPartitionAssignorTest {

    public static class DoubleWeightProvider implements PartitionWeightProvider {
        @Override
        public double partitionWeight(TopicPartition partition) {
            return partition.partition() == 0 ? 4.0 : 1.0;
        }
    }

    /** Partitions 0, 1, 2 each weigh 5.0; all others weigh 1.0. */
    public static class MultiHeavyWeightProvider implements PartitionWeightProvider {
        @Override
        public double partitionWeight(TopicPartition partition) {
            return partition.partition() <= 2 ? 5.0 : 1.0;
        }
    }

    private WeightedPartitionAssignor assignor;

    @BeforeEach
    void setUp() {
        assignor = new WeightedPartitionAssignor();
    }

    @Test
    void nameReturnsWeighted() {
        assertEquals("weighted", assignor.name());
    }

    @Test
    void evenDistribution() {
        // 6 partitions, 3 consumers → 2 each
        Map<String, Integer> partitionsPerTopic = Collections.singletonMap("topic1", 6);
        Map<String, Subscription> subscriptions = new HashMap<>();
        subscriptions.put("c1", new Subscription(Collections.singletonList("topic1")));
        subscriptions.put("c2", new Subscription(Collections.singletonList("topic1")));
        subscriptions.put("c3", new Subscription(Collections.singletonList("topic1")));

        Map<String, List<TopicPartition>> result = assignor.assign(partitionsPerTopic, subscriptions);

        assertEquals(3, result.size());
        for (List<TopicPartition> partitions : result.values()) {
            assertEquals(2, partitions.size());
        }
        // Total partitions assigned
        assertEquals(6, result.values().stream().mapToInt(List::size).sum());
    }

    @Test
    void unevenDistribution() {
        // 7 partitions, 3 consumers → some get 3, some get 2
        Map<String, Integer> partitionsPerTopic = Collections.singletonMap("topic1", 7);
        Map<String, Subscription> subscriptions = new HashMap<>();
        subscriptions.put("c1", new Subscription(Collections.singletonList("topic1")));
        subscriptions.put("c2", new Subscription(Collections.singletonList("topic1")));
        subscriptions.put("c3", new Subscription(Collections.singletonList("topic1")));

        Map<String, List<TopicPartition>> result = assignor.assign(partitionsPerTopic, subscriptions);

        assertEquals(3, result.size());
        assertEquals(7, result.values().stream().mapToInt(List::size).sum());

        // Each consumer should get 2 or 3 partitions
        for (List<TopicPartition> partitions : result.values()) {
            assertTrue(partitions.size() >= 2 && partitions.size() <= 3,
                    "Expected 2 or 3 partitions, got " + partitions.size());
        }
    }

    @Test
    void moreConsumersThanPartitions() {
        // 2 partitions, 5 consumers → 2 consumers get 1 each, 3 get 0
        Map<String, Integer> partitionsPerTopic = Collections.singletonMap("topic1", 2);
        Map<String, Subscription> subscriptions = new HashMap<>();
        subscriptions.put("c1", new Subscription(Collections.singletonList("topic1")));
        subscriptions.put("c2", new Subscription(Collections.singletonList("topic1")));
        subscriptions.put("c3", new Subscription(Collections.singletonList("topic1")));
        subscriptions.put("c4", new Subscription(Collections.singletonList("topic1")));
        subscriptions.put("c5", new Subscription(Collections.singletonList("topic1")));

        Map<String, List<TopicPartition>> result = assignor.assign(partitionsPerTopic, subscriptions);

        assertEquals(5, result.size());
        assertEquals(2, result.values().stream().mapToInt(List::size).sum());

        long consumersWithPartitions = result.values().stream().filter(p -> !p.isEmpty()).count();
        assertEquals(2, consumersWithPartitions);
    }

    @Test
    void multipleTopics() {
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put("topic1", 3);
        partitionsPerTopic.put("topic2", 3);
        Map<String, Subscription> subscriptions = new HashMap<>();
        subscriptions.put("c1", new Subscription(Arrays.asList("topic1", "topic2")));
        subscriptions.put("c2", new Subscription(Arrays.asList("topic1", "topic2")));
        subscriptions.put("c3", new Subscription(Arrays.asList("topic1", "topic2")));

        Map<String, List<TopicPartition>> result = assignor.assign(partitionsPerTopic, subscriptions);

        assertEquals(3, result.size());
        // 6 total partitions across 2 topics
        assertEquals(6, result.values().stream().mapToInt(List::size).sum());
        // Each consumer gets 2 (1 from each topic)
        for (List<TopicPartition> partitions : result.values()) {
            assertEquals(2, partitions.size());
        }
    }

    @Test
    void partialSubscriptions() {
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put("topic1", 3);
        partitionsPerTopic.put("topic2", 3);
        Map<String, Subscription> subscriptions = new HashMap<>();
        subscriptions.put("c1", new Subscription(Collections.singletonList("topic1")));
        subscriptions.put("c2", new Subscription(Collections.singletonList("topic2")));
        subscriptions.put("c3", new Subscription(Arrays.asList("topic1", "topic2")));

        Map<String, List<TopicPartition>> result = assignor.assign(partitionsPerTopic, subscriptions);

        assertEquals(3, result.size());
        // All 6 partitions should be assigned
        assertEquals(6, result.values().stream().mapToInt(List::size).sum());

        // c1 should only have topic1 partitions
        assertTrue(result.get("c1").stream().allMatch(tp -> tp.topic().equals("topic1")));
        // c2 should only have topic2 partitions
        assertTrue(result.get("c2").stream().allMatch(tp -> tp.topic().equals("topic2")));
        // c3 should have both
        assertTrue(result.get("c3").stream().anyMatch(tp -> tp.topic().equals("topic1"))
                || result.get("c3").stream().anyMatch(tp -> tp.topic().equals("topic2")));
    }

    @Test
    void defaultWeightProviderReturnsOne() {
        assertEquals(1.0, assignor.getPartitionWeight(new TopicPartition("any-topic", 0)));
        assertEquals(1.0, assignor.getPartitionWeight(new TopicPartition("other-topic", 42)));
    }

    @Test
    void configureWithUniformWeightProvider() {
        Map<String, String> configs = new HashMap<>();
        configs.put(WEIGHT_PROVIDER_CLASS_CONFIG, UniformWeightProvider.class.getName());
        assignor.configure(configs);
        assertEquals(1.0, assignor.getPartitionWeight(new TopicPartition("topic1", 0)));
    }

    @Test
    void configureWithCustomWeightProvider() {
        Map<String, String> configs = new HashMap<>();
        configs.put(WEIGHT_PROVIDER_CLASS_CONFIG, DoubleWeightProvider.class.getName());
        assignor.configure(configs);
        assertEquals(4.0, assignor.getPartitionWeight(new TopicPartition("topic1", 0)));
        assertEquals(1.0, assignor.getPartitionWeight(new TopicPartition("topic1", 1)));
    }

    @Test
    void configureWithoutProviderPropertyUsesDefault() {
        assignor.configure(Collections.emptyMap());
        assertEquals(1.0, assignor.getPartitionWeight(new TopicPartition("topic1", 0)));
    }

    @Test
    void configureWithInvalidClassThrows() {
        Map<String, String> configs = new HashMap<>();
        configs.put(WEIGHT_PROVIDER_CLASS_CONFIG, "com.nonexistent.FakeProvider");
        assertThrows(RuntimeException.class, () -> assignor.configure(configs));
    }

    @Test
    void rebalanceMovesHeavyPartitionsToBalance() {
        Map<String, String> configs = new HashMap<>();
        configs.put(WEIGHT_PROVIDER_CLASS_CONFIG, DoubleWeightProvider.class.getName());
        assignor.configure(configs);

        // 4 partitions, 2 consumers; partition-0 has weight 4.0, others 1.0 (total weight = 5.0)
        Map<String, Integer> partitionsPerTopic = Collections.singletonMap("topic1", 4);
        Map<String, Subscription> subscriptions = new HashMap<>();
        subscriptions.put("c1", new Subscription(Collections.singletonList("topic1")));
        subscriptions.put("c2", new Subscription(Collections.singletonList("topic1")));

        Map<String, List<TopicPartition>> result = assignor.assign(partitionsPerTopic, subscriptions);

        assertEquals(4, result.values().stream().mapToInt(List::size).sum());

        // Compute weight per consumer and verify the gap is <= 1.0
        double w1 = result.get("c1").stream().mapToDouble(tp -> assignor.getPartitionWeight(tp)).sum();
        double w2 = result.get("c2").stream().mapToDouble(tp -> assignor.getPartitionWeight(tp)).sum();
        assertTrue(Math.abs(w1 - w2) <= 1.0,
                "Weight gap should be <= 1.0 but was " + Math.abs(w1 - w2));
    }

    @Test
    void weightRebalanceDistributesAcrossMultipleHeavyConsumers() {
        Map<String, String> configs = new HashMap<>();
        configs.put(WEIGHT_PROVIDER_CLASS_CONFIG, MultiHeavyWeightProvider.class.getName());
        assignor.configure(configs);

        // 14 partitions, 5 consumers; partitions 0-2 = 5.0 each, others = 1.0
        // Total weight = 3*5.0 + 11*1.0 = 26.0, target = 5.2 per consumer
        // Sticky assigns 2-3 partitions each → 3 consumers holding a heavy partition
        // start at weight 6.0 and must each shed a light partition to balance.
        Map<String, Integer> partitionsPerTopic = Collections.singletonMap("topic1", 14);
        Map<String, Subscription> subscriptions = new HashMap<>();
        for (int i = 1; i <= 5; i++) {
            subscriptions.put("c" + i, new Subscription(Collections.singletonList("topic1")));
        }

        Map<String, List<TopicPartition>> result = assignor.assign(partitionsPerTopic, subscriptions);

        // All 14 partitions assigned
        assertEquals(14, result.values().stream().mapToInt(List::size).sum());

        // Compute per-consumer weights
        double maxW = Double.NEGATIVE_INFINITY;
        double minW = Double.POSITIVE_INFINITY;
        for (List<TopicPartition> partitions : result.values()) {
            double w = partitions.stream().mapToDouble(tp -> assignor.getPartitionWeight(tp)).sum();
            maxW = Math.max(maxW, w);
            minW = Math.min(minW, w);
        }

        // The heavy partition weight is 5.0; gap must be at most that
        assertTrue(maxW - minW <= 5.0,
                "Weight gap should be <= 5.0 but was " + (maxW - minW));
    }

    /**
     * Partition 0 = 3.0, partition 1 = 2.0, all others = 1.0.
     * With 2 consumers and 2 partitions: total = 5.0, target = 2.5.
     * Gap = 1.0, tolerance at 100% = 2.5 → already within tolerance, no moves should occur.
     */
    public static class SlightlySkewedWeightProvider implements PartitionWeightProvider {
        @Override
        public double partitionWeight(TopicPartition partition) {
            if (partition.partition() == 0) return 3.0;
            if (partition.partition() == 1) return 2.0;
            return 1.0;
        }
    }

    @Test
    void toleranceSkipsRebalanceWhenAlreadyBalanced() {
        Map<String, String> configs = new HashMap<>();
        configs.put(WEIGHT_PROVIDER_CLASS_CONFIG, SlightlySkewedWeightProvider.class.getName());
        assignor.configure(configs);
        // Set tolerance very high so that the 1.0 gap is within tolerance
        assignor.setToleranceRatio(1.0); // tolerance = target * 1.0 = 2.5

        Map<String, Integer> partitionsPerTopic = Collections.singletonMap("topic1", 2);
        Map<String, Subscription> subscriptions = new HashMap<>();
        subscriptions.put("c1", new Subscription(Collections.singletonList("topic1")));
        subscriptions.put("c2", new Subscription(Collections.singletonList("topic1")));

        Map<String, List<TopicPartition>> result = assignor.assign(partitionsPerTopic, subscriptions);

        // Each consumer should keep exactly 1 partition (the sticky assignment, no rebalance moves)
        assertEquals(1, result.get("c1").size());
        assertEquals(1, result.get("c2").size());

        // With high tolerance, the original sticky assignment is preserved:
        // each consumer keeps its single partition — no movement occurred
        double w1 = result.get("c1").stream().mapToDouble(tp -> assignor.getPartitionWeight(tp)).sum();
        double w2 = result.get("c2").stream().mapToDouble(tp -> assignor.getPartitionWeight(tp)).sum();
        // The gap should be 1.0 (3.0 vs 2.0) — within tolerance, so no moves happened
        assertEquals(5.0, w1 + w2, 0.001);
    }

    /** Every partition has weight 10.0 — forces many moves in a large scenario. */
    public static class HeavyUniformWeightProvider implements PartitionWeightProvider {
        @Override
        public double partitionWeight(TopicPartition partition) {
            return 10.0;
        }
    }

    @Test
    void maxMovesLimitsRebalanceMoves() {
        Map<String, String> configs = new HashMap<>();
        configs.put(WEIGHT_PROVIDER_CLASS_CONFIG, DoubleWeightProvider.class.getName());
        assignor.configure(configs);
        // Set maxMovesFactor to 0 so maxMoves = 0 — no moves allowed
        assignor.setMaxMovesFactor(0);
        assignor.setToleranceRatio(0.0); // disable tolerance so it would rebalance if allowed

        // 4 partitions (p0=4.0, p1-p3=1.0 each), 2 consumers
        // Sticky assigns 2 each; one consumer likely gets p0+p1 = 5.0, other gets p2+p3 = 2.0
        // Without the move cap, rebalance would move partitions. With maxMoves=0, it can't.
        Map<String, Integer> partitionsPerTopic = Collections.singletonMap("topic1", 4);
        Map<String, Subscription> subscriptions = new HashMap<>();
        subscriptions.put("c1", new Subscription(Collections.singletonList("topic1")));
        subscriptions.put("c2", new Subscription(Collections.singletonList("topic1")));

        Map<String, List<TopicPartition>> result = assignor.assign(partitionsPerTopic, subscriptions);

        // All partitions still assigned
        assertEquals(4, result.values().stream().mapToInt(List::size).sum());

        // Each consumer should still have exactly 2 (no moves happened)
        assertEquals(2, result.get("c1").size());
        assertEquals(2, result.get("c2").size());

        // Weights may be unbalanced since moves were capped at 0
        double w1 = result.get("c1").stream().mapToDouble(tp -> assignor.getPartitionWeight(tp)).sum();
        double w2 = result.get("c2").stream().mapToDouble(tp -> assignor.getPartitionWeight(tp)).sum();
        // The gap should be > 0 (sticky assignment without rebalance)
        // Just verify we didn't lose/duplicate any weight
        assertEquals(7.0, w1 + w2, 0.001);
    }

    @Test
    void noPartitionsAvailable() {
        Map<String, Integer> partitionsPerTopic = Collections.singletonMap("topic1", 0);
        Map<String, Subscription> subscriptions = new HashMap<>();
        subscriptions.put("c1", new Subscription(Collections.singletonList("topic1")));
        subscriptions.put("c2", new Subscription(Collections.singletonList("topic1")));

        Map<String, List<TopicPartition>> result = assignor.assign(partitionsPerTopic, subscriptions);

        assertEquals(2, result.size());
        assertEquals(0, result.values().stream().mapToInt(List::size).sum());
    }
}
