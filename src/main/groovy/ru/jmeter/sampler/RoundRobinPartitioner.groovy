package ru.sberbank.atmproc.jmeter.sampler

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.utils.Utils
import java.util.concurrent.atomic.AtomicInteger

public class RoundRobinPartitioner implements Partitioner {

    private final AtomicInteger counter = new AtomicInteger(0)

    @Override
    int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic)
        int numPartitions = partitions.size()

        if (numPartitions <= 0) return 0
        if (key) return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions

        return (counter.getAndIncrement() & Integer.MAX_VALUE) % numPartitions
    }

    @Override
    void close() {}

    @Override
    void configure(Map<String, ?> configs) {}
}
