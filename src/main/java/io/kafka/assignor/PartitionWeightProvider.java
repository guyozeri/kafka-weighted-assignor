package io.kafka.assignor;

import org.apache.kafka.common.TopicPartition;

import java.io.Closeable;
import java.util.Map;

public interface PartitionWeightProvider extends Closeable {

    double partitionWeight(TopicPartition partition);

    default void configure(Map<String, ?> configs) {
    }

    @Override
    default void close() {
    }
}
