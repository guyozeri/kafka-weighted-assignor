package io.github.guyozeri.kafka.assignor;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class WeightedPartitionAssignor extends AbstractStickyAssignor implements Configurable {

    // --- Public config constant ---
    public static final String WEIGHT_PROVIDER_CLASS_CONFIG = "weighted.assignor.weight.provider.class";

    // --- Rebalance tuning constants and fields ---
    static final double DEFAULT_TOLERANCE_RATIO = 0.05;
    static final int DEFAULT_MAX_MOVES_FACTOR = 2;

    private double toleranceRatio = DEFAULT_TOLERANCE_RATIO;
    private int maxMovesFactor = DEFAULT_MAX_MOVES_FACTOR;

    void setToleranceRatio(double toleranceRatio) {
        this.toleranceRatio = toleranceRatio;
    }

    void setMaxMovesFactor(int maxMovesFactor) {
        this.maxMovesFactor = maxMovesFactor;
    }

    // --- Comparator ---
    private static final Comparator<TopicPartition> TP_COMPARATOR =
            Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition);

    // --- Instance state ---
    private List<TopicPartition> memberAssignment = Collections.emptyList();
    private int generation = DEFAULT_GENERATION;
    private PartitionWeightProvider weightProvider = new UniformWeightProvider();

    @Override
    public String name() {
        return "weighted";
    }

    @Override
    protected MemberData memberData(ConsumerPartitionAssignor.Subscription subscription) {
        ByteBuffer userData = subscription.userData();
        if (userData == null) {
            return new MemberData(Collections.emptyList(), Optional.empty());
        }
        return AssignmentSerializer.deserialize(userData);
    }

    @Override
    public void onAssignment(ConsumerPartitionAssignor.Assignment assignment, ConsumerGroupMetadata metadata) {
        this.memberAssignment = assignment.partitions();
        this.generation = metadata.generationId();
    }

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                     Map<String, Subscription> subscriptions) {
        Map<String, List<TopicPartition>> assignment = super.assign(partitionsPerTopic, subscriptions);
        rebalanceByWeight(assignment, subscriptions);
        return assignment;
    }

    public void configure(Map<String, ?> configs) {
        Object value = configs.get(WEIGHT_PROVIDER_CLASS_CONFIG);
        if (value != null) {
            String className = value.toString();
            try {
                Class<?> clazz = Class.forName(className);
                PartitionWeightProvider provider = (PartitionWeightProvider) clazz.getDeclaredConstructor().newInstance();
                provider.configure(configs);
                this.weightProvider = provider;
            } catch (Exception e) {
                throw new RuntimeException("Failed to instantiate weight provider: " + className, e);
            }
        }
    }

    double getPartitionWeight(TopicPartition partition) {
        return weightProvider.partitionWeight(partition);
    }

    @Override
    public ByteBuffer subscriptionUserData(Set<String> topics) {
        if (memberAssignment.isEmpty() && generation == DEFAULT_GENERATION) {
            return null;
        }
        return AssignmentSerializer.serialize(memberAssignment, generation);
    }

    // --- Rebalancing algorithm helpers ---

    private static class WeightSummary {
        final Map<String, Double> weights;
        final double totalWeight;
        final int totalPartitions;

        WeightSummary(Map<String, Double> weights, double totalWeight, int totalPartitions) {
            this.weights = weights;
            this.totalWeight = totalWeight;
            this.totalPartitions = totalPartitions;
        }
    }

    private static class MoveCandidate {
        final TopicPartition partition;
        final int index;

        MoveCandidate(TopicPartition partition, int index) {
            this.partition = partition;
            this.index = index;
        }
    }

    private WeightSummary computeConsumerWeights(Map<String, List<TopicPartition>> assignment) {
        Map<String, Double> weights = new HashMap<>();
        double totalWeight = 0.0;
        int totalPartitions = 0;
        for (Map.Entry<String, List<TopicPartition>> entry : assignment.entrySet()) {
            double sum = 0.0;
            for (TopicPartition tp : entry.getValue()) {
                sum += getPartitionWeight(tp);
            }
            weights.put(entry.getKey(), sum);
            totalWeight += sum;
            totalPartitions += entry.getValue().size();
        }
        return new WeightSummary(weights, totalWeight, totalPartitions);
    }

    private static List<String> sortConsumersByWeightDesc(Map<String, Double> weights) {
        List<String> sorted = new ArrayList<>(weights.keySet());
        sorted.sort(Comparator.<String, Double>comparing(weights::get).reversed()
                .thenComparing(Comparator.naturalOrder()));
        return sorted;
    }

    private static String findLightestConsumer(List<String> sorted, String source, Map<String, Double> weights) {
        String bestDest = null;
        double bestDestWeight = Double.MAX_VALUE;
        for (String dest : sorted) {
            if (dest.equals(source)) {
                continue;
            }
            double destWeight = weights.get(dest);
            if (destWeight < bestDestWeight) {
                bestDest = dest;
                bestDestWeight = destWeight;
            }
        }
        return bestDest;
    }

    private MoveCandidate findBestPartitionToMove(List<TopicPartition> sourcePartitions,
                                                   Set<String> destTopics, double destWeight,
                                                   double sourceWeight, double gap) {
        TopicPartition bestCandidate = null;
        int bestCandidateIdx = -1;
        double bestDistance = Double.MAX_VALUE;

        for (int j = 0; j < sourcePartitions.size(); j++) {
            TopicPartition candidate = sourcePartitions.get(j);
            if (!destTopics.contains(candidate.topic())) {
                continue;
            }
            double pw = getPartitionWeight(candidate);
            if (destWeight + pw >= sourceWeight - pw) {
                continue;
            }
            double distance = Math.abs(pw - gap / 2.0);
            if (distance < bestDistance || (distance == bestDistance && bestCandidate != null
                    && TP_COMPARATOR.compare(candidate, bestCandidate) < 0)) {
                bestCandidate = candidate;
                bestCandidateIdx = j;
                bestDistance = distance;
            }
        }

        return bestCandidate != null ? new MoveCandidate(bestCandidate, bestCandidateIdx) : null;
    }

    private void rebalanceByWeight(Map<String, List<TopicPartition>> assignment,
                                      Map<String, Subscription> subscriptions) {
        WeightSummary summary = computeConsumerWeights(assignment);
        Map<String, Double> weights = summary.weights;

        int numConsumers = assignment.size();
        if (numConsumers == 0) {
            return;
        }
        double target = summary.totalWeight / numConsumers;
        double tolerance = target * toleranceRatio;
        int maxMoves = summary.totalPartitions * maxMovesFactor;
        int moveCount = 0;

        while (moveCount < maxMoves) {
            boolean movedAny = false;
            List<String> sorted = sortConsumersByWeightDesc(weights);

            String heaviest = sorted.get(0);
            String lightest = sorted.get(sorted.size() - 1);
            if (weights.get(heaviest) - weights.get(lightest) <= tolerance) {
                break;
            }

            for (String source : sorted) {
                if (moveCount >= maxMoves) {
                    break;
                }
                double sourceWeight = weights.get(source);
                if (sourceWeight <= target) {
                    break;
                }

                List<TopicPartition> sourcePartitions = assignment.get(source);
                sourcePartitions.sort(TP_COMPARATOR);

                while (moveCount < maxMoves) {
                    String bestDest = findLightestConsumer(sorted, source, weights);
                    if (bestDest == null) {
                        break;
                    }

                    double destWeight = weights.get(bestDest);
                    double gap = sourceWeight - destWeight;
                    Set<String> destTopics = new HashSet<>(subscriptions.get(bestDest).topics());

                    MoveCandidate candidate = findBestPartitionToMove(
                            sourcePartitions, destTopics, destWeight, sourceWeight, gap);
                    if (candidate == null) {
                        break;
                    }

                    double pw = getPartitionWeight(candidate.partition);
                    sourcePartitions.remove(candidate.index);
                    assignment.get(bestDest).add(candidate.partition);
                    sourceWeight -= pw;
                    weights.put(source, sourceWeight);
                    weights.put(bestDest, destWeight + pw);
                    movedAny = true;
                    moveCount++;
                }
            }

            if (!movedAny) {
                break;
            }
        }
    }

    // --- Assignment serialization ---

    private static class AssignmentSerializer {

        // Binary format: [generation:int][count:int][topic-len:short, topic-bytes, partition:int]*
        static ByteBuffer serialize(List<TopicPartition> partitions, int generation) {
            int size = Integer.BYTES + Integer.BYTES;
            List<byte[]> topicBytes = new ArrayList<>(partitions.size());
            for (TopicPartition tp : partitions) {
                byte[] bytes = tp.topic().getBytes(StandardCharsets.UTF_8);
                topicBytes.add(bytes);
                size += Short.BYTES + bytes.length + Integer.BYTES;
            }

            ByteBuffer buffer = ByteBuffer.allocate(size);
            buffer.putInt(generation);
            buffer.putInt(partitions.size());
            for (int i = 0; i < partitions.size(); i++) {
                byte[] tb = topicBytes.get(i);
                buffer.putShort((short) tb.length);
                buffer.put(tb);
                buffer.putInt(partitions.get(i).partition());
            }
            buffer.flip();
            return buffer;
        }

        static MemberData deserialize(ByteBuffer buffer) {
            buffer = buffer.slice();
            int gen = buffer.getInt();
            int count = buffer.getInt();
            List<TopicPartition> partitions = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                short topicLen = buffer.getShort();
                byte[] topicBytes = new byte[topicLen];
                buffer.get(topicBytes);
                String topic = new String(topicBytes, StandardCharsets.UTF_8);
                int partition = buffer.getInt();
                partitions.add(new TopicPartition(topic, partition));
            }
            return new MemberData(partitions, Optional.of(gen));
        }
    }
}
