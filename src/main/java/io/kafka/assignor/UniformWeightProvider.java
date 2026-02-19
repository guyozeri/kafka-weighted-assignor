package io.kafka.assignor;

import org.apache.kafka.common.TopicPartition;

public class UniformWeightProvider implements PartitionWeightProvider {

    @Override
    public double partitionWeight(TopicPartition partition) {
        return 1.0;
    }
}
