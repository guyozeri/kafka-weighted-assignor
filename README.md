# kafka-weighted-assignor

A Kafka partition assignor that distributes partitions based on configurable weights rather than simple round-robin count balancing. When some partitions carry significantly more traffic than others, the weighted assignor moves light partitions to compensate, producing uneven partition counts but balanced overall load.

## How it works

`WeightedPartitionAssignor` extends Kafka's sticky assignor. After the initial sticky assignment, it runs a rebalancing pass that:

1. Computes total weight per consumer using a pluggable `PartitionWeightProvider`
2. Calculates a target weight (total / number of consumers)
3. Iteratively moves partitions from overloaded consumers to underloaded ones

The result is that consumers with a high-throughput partition end up with fewer total partitions, while consumers without one absorb more light partitions.

**Example:** 10 partitions across 4 consumers, where p0 and p1 have 100x the throughput of the rest:

| Assignor | consumer-0 | consumer-1 | consumer-2 | consumer-3 |
|---|---|---|---|---|
| Round-robin | p0,p4,p8 (3) | p1,p5,p9 (3) | p2,p6 (2) | p3,p7 (2) |
| **Weighted** | **p0 (1)** | **p1 (1)** | **p2,p4,p5,p6 (4)** | **p3,p7,p8,p9 (4)** |

## Quick start

Add the dependency (the library requires `kafka-clients` on the classpath):

```xml
<dependency>
    <groupId>io.github.guyozeri.kafka.assignor</groupId>
    <artifactId>kafka-weighted-assignor</artifactId>
    <version>1.0.0</version>
</dependency>
```

Configure your consumer:

```java
Properties props = new Properties();
props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

// Use the weighted assignor
props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
        WeightedPartitionAssignor.class.getName());

// Use throughput-based weighting
props.put(WeightedPartitionAssignor.WEIGHT_PROVIDER_CLASS_CONFIG,
        ThroughputWeightProvider.class.getName());

KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
```

See [`examples/WeightedConsumerExample.java`](examples/WeightedConsumerExample.java) for a full working example.

## Weight providers

### ThroughputWeightProvider

Weights partitions by their message throughput. On each rebalance it samples partition offsets via the Admin API, waits a configurable interval, samples again, and computes messages/second.

| Config | Default | Description |
|---|---|---|
| `throughput.weight.sampling.interval.ms` | `2000` | Time between the two offset samples |
| `throughput.weight.min.weight` | `1.0` | Floor weight for idle partitions |
| `throughput.weight.background.period.ms` | `30000` | How often to re-sample all known topics in the background. Set to `0` to disable. |

### UniformWeightProvider

Returns `1.0` for all partitions. This is the default when no provider is configured, making the assignor behave like a standard sticky assignor.

### Custom providers

Implement the `PartitionWeightProvider` interface:

```java
public class MyWeightProvider implements PartitionWeightProvider {

    @Override
    public double partitionWeight(TopicPartition partition) {
        // return a weight based on your own logic
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // read any custom configs from the consumer properties
    }
}
```

Then set:

```java
props.put(WeightedPartitionAssignor.WEIGHT_PROVIDER_CLASS_CONFIG,
        MyWeightProvider.class.getName());
```

## Configuration reference

| Config | Required | Description |
|---|---|---|
| `weighted.assignor.weight.provider.class` | No | Fully qualified class name of the `PartitionWeightProvider` implementation. Defaults to `UniformWeightProvider`. |
| `throughput.weight.sampling.interval.ms` | No | Sampling interval for `ThroughputWeightProvider` (default `2000`). |
| `throughput.weight.min.weight` | No | Minimum weight for `ThroughputWeightProvider` (default `1.0`). |
| `throughput.weight.background.period.ms` | No | Background re-sampling period for `ThroughputWeightProvider` (default `30000`). Set to `0` to disable. |

## Building

```bash
make build        # compile
make package      # clean + build JAR
make install      # clean + install to local Maven repo
```

## Testing

Unit tests (no Docker required):

```bash
make unit-test
```

Full test suite including the system test (requires Docker):

```bash
make test
```

The system test spins up a real Kafka broker via Testcontainers, produces messages at uneven rates across partitions, and verifies that the weighted assignor produces uneven partition counts to balance throughput.

## Requirements

- Java 8+
- Kafka clients 2.5.0+
- Docker (for system tests only)
