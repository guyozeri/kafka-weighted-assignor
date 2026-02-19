package io.kafka.assignor;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.*;
import static io.kafka.assignor.ThroughputWeightProvider.*;

class ThroughputWeightProviderTest {

    static class TestableThroughputWeightProvider extends ThroughputWeightProvider {

        private List<TopicPartitionInfo> partitionInfos;
        private final Map<TopicPartition, Long> offsets1 = new HashMap<>();
        private final Map<TopicPartition, Long> offsets2 = new HashMap<>();
        private int fetchCallCount = 0;
        private int describeCallCount = 0;
        private long totalSleepMs = 0;
        private boolean describeThrows = false;

        void setPartitionInfos(List<TopicPartitionInfo> infos) {
            this.partitionInfos = infos;
        }

        void setOffsets(Map<TopicPartition, Long> first, Map<TopicPartition, Long> second) {
            offsets1.clear();
            offsets1.putAll(first);
            offsets2.clear();
            offsets2.putAll(second);
        }

        void setDescribeThrows(boolean describeThrows) {
            this.describeThrows = describeThrows;
        }

        @Override
        ScheduledExecutorService createScheduler() {
            return null; // disable background sampling in tests
        }

        @Override
        Admin createAdminClient(Map<String, ?> configs) {
            return null;
        }

        @Override
        List<TopicPartitionInfo> describeTopicPartitions(String topic) throws Exception {
            describeCallCount++;
            if (describeThrows) {
                throw new RuntimeException("Simulated describe failure");
            }
            return partitionInfos;
        }

        @Override
        Map<TopicPartition, Long> fetchOffsets(Map<TopicPartition, OffsetSpec> request) {
            fetchCallCount++;
            return fetchCallCount % 2 == 1 ? new HashMap<>(offsets1) : new HashMap<>(offsets2);
        }

        @Override
        void sleep(long ms) {
            totalSleepMs += ms;
        }
    }

    private static final Node NO_LEADER = null;

    private static TopicPartitionInfo partitionInfo(int id) {
        return new TopicPartitionInfo(id, NO_LEADER, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    void rateComputation() {
        TestableThroughputWeightProvider provider = new TestableThroughputWeightProvider();
        provider.configure(Collections.emptyMap());

        provider.setPartitionInfos(Arrays.asList(partitionInfo(0), partitionInfo(1)));

        TopicPartition tp0 = new TopicPartition("topic1", 0);
        TopicPartition tp1 = new TopicPartition("topic1", 1);

        Map<TopicPartition, Long> first = new HashMap<>();
        first.put(tp0, 100L);
        first.put(tp1, 200L);
        Map<TopicPartition, Long> second = new HashMap<>();
        second.put(tp0, 300L);
        second.put(tp1, 400L);
        provider.setOffsets(first, second);

        // Default interval is 2000ms = 2sec, so rate = 200/2 = 100 msg/s
        assertEquals(100.0, provider.partitionWeight(tp0), 0.001);
        assertEquals(100.0, provider.partitionWeight(tp1), 0.001);
    }

    @Test
    void unevenThroughput() {
        TestableThroughputWeightProvider provider = new TestableThroughputWeightProvider();
        provider.configure(Collections.emptyMap());

        provider.setPartitionInfos(Arrays.asList(partitionInfo(0), partitionInfo(1)));

        TopicPartition tp0 = new TopicPartition("topic1", 0);
        TopicPartition tp1 = new TopicPartition("topic1", 1);

        Map<TopicPartition, Long> first = new HashMap<>();
        first.put(tp0, 0L);
        first.put(tp1, 0L);
        Map<TopicPartition, Long> second = new HashMap<>();
        second.put(tp0, 2000L);  // 2000/2 = 1000 msg/s
        second.put(tp1, 20L);    // 20/2 = 10 msg/s
        provider.setOffsets(first, second);

        double w0 = provider.partitionWeight(tp0);
        double w1 = provider.partitionWeight(tp1);
        assertEquals(1000.0, w0, 0.001);
        assertEquals(10.0, w1, 0.001);
        assertEquals(100.0, w0 / w1, 0.001);
    }

    @Test
    void idlePartitionGetsMinWeight() {
        TestableThroughputWeightProvider provider = new TestableThroughputWeightProvider();
        provider.configure(Collections.emptyMap());

        provider.setPartitionInfos(Collections.singletonList(partitionInfo(0)));

        TopicPartition tp0 = new TopicPartition("topic1", 0);

        Map<TopicPartition, Long> first = new HashMap<>();
        first.put(tp0, 500L);
        Map<TopicPartition, Long> second = new HashMap<>();
        second.put(tp0, 500L);  // no change → rate = 0, floor to minWeight
        provider.setOffsets(first, second);

        assertEquals(1.0, provider.partitionWeight(tp0), 0.001);
    }

    @Test
    void unknownPartitionReturnsMinWeight() {
        TestableThroughputWeightProvider provider = new TestableThroughputWeightProvider();
        provider.configure(Collections.emptyMap());

        provider.setPartitionInfos(Collections.singletonList(partitionInfo(0)));

        TopicPartition tp0 = new TopicPartition("topic1", 0);
        TopicPartition tp9 = new TopicPartition("topic1", 9);

        Map<TopicPartition, Long> first = new HashMap<>();
        first.put(tp0, 0L);
        Map<TopicPartition, Long> second = new HashMap<>();
        second.put(tp0, 100L);
        provider.setOffsets(first, second);

        // tp9 is not in the offsets results, so it should get minWeight
        provider.partitionWeight(tp0); // triggers sampling
        assertEquals(1.0, provider.partitionWeight(tp9), 0.001);
    }

    @Test
    void topicSampledOnlyOnce() {
        TestableThroughputWeightProvider provider = new TestableThroughputWeightProvider();
        provider.configure(Collections.emptyMap());

        provider.setPartitionInfos(Arrays.asList(partitionInfo(0), partitionInfo(1), partitionInfo(2)));

        TopicPartition tp0 = new TopicPartition("topic1", 0);
        TopicPartition tp1 = new TopicPartition("topic1", 1);
        TopicPartition tp2 = new TopicPartition("topic1", 2);

        Map<TopicPartition, Long> first = new HashMap<>();
        first.put(tp0, 0L);
        first.put(tp1, 0L);
        first.put(tp2, 0L);
        Map<TopicPartition, Long> second = new HashMap<>();
        second.put(tp0, 100L);
        second.put(tp1, 100L);
        second.put(tp2, 100L);
        provider.setOffsets(first, second);

        provider.partitionWeight(tp0);
        provider.partitionWeight(tp1);
        provider.partitionWeight(tp2);

        assertEquals(1, provider.describeCallCount);
    }

    @Test
    void errorHandlingReturnsMinWeight() {
        TestableThroughputWeightProvider provider = new TestableThroughputWeightProvider();
        provider.configure(Collections.emptyMap());

        provider.setDescribeThrows(true);

        TopicPartition tp0 = new TopicPartition("topic1", 0);

        // Should not throw, and should return minWeight
        double weight = provider.partitionWeight(tp0);
        assertEquals(1.0, weight, 0.001);
    }

    @Test
    void customConfig() {
        TestableThroughputWeightProvider provider = new TestableThroughputWeightProvider();

        Map<String, String> configs = new HashMap<>();
        configs.put(SAMPLING_INTERVAL_MS_CONFIG, "5000");
        configs.put(MIN_WEIGHT_CONFIG, "2.5");
        provider.configure(configs);

        provider.setPartitionInfos(Collections.singletonList(partitionInfo(0)));

        TopicPartition tp0 = new TopicPartition("topic1", 0);

        Map<TopicPartition, Long> first = new HashMap<>();
        first.put(tp0, 0L);
        Map<TopicPartition, Long> second = new HashMap<>();
        second.put(tp0, 0L);  // idle → should use custom minWeight 2.5
        provider.setOffsets(first, second);

        assertEquals(2.5, provider.partitionWeight(tp0), 0.001);

        // Verify sleep was called with the custom interval
        assertEquals(5000, provider.totalSleepMs);
    }

    @Test
    void backgroundSamplingRefreshesWeights() {
        TestableThroughputWeightProvider provider = new TestableThroughputWeightProvider();
        provider.configure(Collections.emptyMap());

        provider.setPartitionInfos(Arrays.asList(partitionInfo(0), partitionInfo(1)));

        TopicPartition tp0 = new TopicPartition("topic1", 0);
        TopicPartition tp1 = new TopicPartition("topic1", 1);

        // Initial offsets: rate = 200/2 = 100 msg/s
        Map<TopicPartition, Long> first = new HashMap<>();
        first.put(tp0, 100L);
        first.put(tp1, 200L);
        Map<TopicPartition, Long> second = new HashMap<>();
        second.put(tp0, 300L);
        second.put(tp1, 400L);
        provider.setOffsets(first, second);

        // First synchronous sample
        assertEquals(100.0, provider.partitionWeight(tp0), 0.001);
        assertEquals(100.0, provider.partitionWeight(tp1), 0.001);

        // Simulate changed throughput: rate = 1000/2 = 500 msg/s
        provider.fetchCallCount = 0;
        Map<TopicPartition, Long> newFirst = new HashMap<>();
        newFirst.put(tp0, 300L);
        newFirst.put(tp1, 400L);
        Map<TopicPartition, Long> newSecond = new HashMap<>();
        newSecond.put(tp0, 1300L);
        newSecond.put(tp1, 1400L);
        provider.setOffsets(newFirst, newSecond);

        // Manually trigger re-sample (simulates what background thread would do)
        provider.sampleTopic("topic1");

        // Weights should be updated to 500 msg/s
        assertEquals(500.0, provider.partitionWeight(tp0), 0.001);
        assertEquals(500.0, provider.partitionWeight(tp1), 0.001);
    }

    @Test
    void backgroundSamplingDisabledWhenPeriodZero() {
        TestableThroughputWeightProvider provider = new TestableThroughputWeightProvider() {
            boolean schedulerCreated = false;

            @Override
            ScheduledExecutorService createScheduler() {
                schedulerCreated = false; // still null, but we track it
                return null;
            }
        };

        Map<String, String> configs = new HashMap<>();
        configs.put(BACKGROUND_PERIOD_MS_CONFIG, "0");
        provider.configure(configs);

        provider.setPartitionInfos(Collections.singletonList(partitionInfo(0)));

        TopicPartition tp0 = new TopicPartition("topic1", 0);

        Map<TopicPartition, Long> first = new HashMap<>();
        first.put(tp0, 0L);
        Map<TopicPartition, Long> second = new HashMap<>();
        second.put(tp0, 100L);
        provider.setOffsets(first, second);

        // This should work fine - no background sampling attempted
        double weight = provider.partitionWeight(tp0);
        assertEquals(50.0, weight, 0.001);

        // No exceptions thrown, background sampling not started
    }
}
