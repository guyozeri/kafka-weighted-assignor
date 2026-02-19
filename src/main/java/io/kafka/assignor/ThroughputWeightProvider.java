package io.kafka.assignor;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ThroughputWeightProvider implements PartitionWeightProvider {

    private static final Logger LOG = Logger.getLogger(ThroughputWeightProvider.class.getName());

    public static final String SAMPLING_INTERVAL_MS_CONFIG = "throughput.weight.sampling.interval.ms";
    public static final String MIN_WEIGHT_CONFIG = "throughput.weight.min.weight";
    public static final String BACKGROUND_PERIOD_MS_CONFIG = "throughput.weight.background.period.ms";

    private static final long DEFAULT_SAMPLING_INTERVAL_MS = 2000;
    private static final double DEFAULT_MIN_WEIGHT = 1.0;
    private static final long DEFAULT_BACKGROUND_PERIOD_MS = 30000;

    private Admin adminClient;
    private long samplingIntervalMs = DEFAULT_SAMPLING_INTERVAL_MS;
    private double minWeight = DEFAULT_MIN_WEIGHT;
    private long backgroundPeriodMs = DEFAULT_BACKGROUND_PERIOD_MS;
    private final Map<TopicPartition, Double> throughputCache = new ConcurrentHashMap<>();
    private final Set<String> sampledTopics = new HashSet<>();
    private ScheduledExecutorService scheduler;
    private boolean backgroundSamplingStarted = false;

    @Override
    public void configure(Map<String, ?> configs) {
        Object intervalVal = configs.get(SAMPLING_INTERVAL_MS_CONFIG);
        if (intervalVal != null) {
            samplingIntervalMs = Long.parseLong(intervalVal.toString());
        }

        Object minWeightVal = configs.get(MIN_WEIGHT_CONFIG);
        if (minWeightVal != null) {
            minWeight = Double.parseDouble(minWeightVal.toString());
        }

        Object backgroundVal = configs.get(BACKGROUND_PERIOD_MS_CONFIG);
        if (backgroundVal != null) {
            backgroundPeriodMs = Long.parseLong(backgroundVal.toString());
        }

        scheduler = createScheduler();
        adminClient = createAdminClient(configs);
    }

    @Override
    public double partitionWeight(TopicPartition partition) {
        if (!sampledTopics.contains(partition.topic())) {
            sampleTopic(partition.topic());
        }
        return throughputCache.getOrDefault(partition, minWeight);
    }

    @Override
    public void close() {
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        if (adminClient != null) {
            adminClient.close();
        }
    }

    void sampleTopic(String topic) {
        try {
            List<TopicPartitionInfo> partitionInfos = describeTopicPartitions(topic);

            Map<TopicPartition, OffsetSpec> request = new HashMap<>();
            for (TopicPartitionInfo info : partitionInfos) {
                request.put(new TopicPartition(topic, info.partition()), OffsetSpec.latest());
            }

            Map<TopicPartition, Long> offsets1 = fetchOffsets(request);
            sleep(samplingIntervalMs);
            Map<TopicPartition, Long> offsets2 = fetchOffsets(request);

            double intervalSec = samplingIntervalMs / 1000.0;
            for (Map.Entry<TopicPartition, Long> entry : offsets2.entrySet()) {
                TopicPartition tp = entry.getKey();
                long o1 = offsets1.getOrDefault(tp, 0L);
                long o2 = entry.getValue();
                double rate = (o2 - o1) / intervalSec;
                throughputCache.put(tp, Math.max(rate, minWeight));
            }
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Failed to sample throughput for topic " + topic, e);
        } finally {
            sampledTopics.add(topic);
        }
        startBackgroundSampling();
    }

    private void startBackgroundSampling() {
        if (backgroundSamplingStarted || backgroundPeriodMs <= 0 || scheduler == null) {
            return;
        }
        backgroundSamplingStarted = true;
        scheduler.scheduleWithFixedDelay(() -> {
            for (String topic : sampledTopics) {
                try {
                    sampleTopic(topic);
                } catch (Exception e) {
                    LOG.log(Level.WARNING, "Background sampling failed for topic " + topic, e);
                }
            }
        }, backgroundPeriodMs, backgroundPeriodMs, TimeUnit.MILLISECONDS);
    }

    ScheduledExecutorService createScheduler() {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "throughput-weight-sampler");
            t.setDaemon(true);
            return t;
        });
    }

    Admin createAdminClient(Map<String, ?> configs) {
        Map<String, Object> adminConfigs = new HashMap<>();
        for (Map.Entry<String, ?> entry : configs.entrySet()) {
            adminConfigs.put(entry.getKey(), entry.getValue());
        }
        return AdminClient.create(adminConfigs);
    }

    List<TopicPartitionInfo> describeTopicPartitions(String topic) throws Exception {
        TopicDescription description = adminClient.describeTopics(Collections.singletonList(topic))
                .all().get().get(topic);
        return description.partitions();
    }

    Map<TopicPartition, Long> fetchOffsets(Map<TopicPartition, OffsetSpec> request) throws Exception {
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> result =
                adminClient.listOffsets(request).all().get();
        Map<TopicPartition, Long> offsets = new HashMap<>();
        for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> entry : result.entrySet()) {
            offsets.put(entry.getKey(), entry.getValue().offset());
        }
        return offsets;
    }

    void sleep(long ms) throws InterruptedException {
        Thread.sleep(ms);
    }
}
