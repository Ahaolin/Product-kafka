package com.jujin.kafka.kafkademo.char2.producer.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author MSI
 * 分区器
 * //代码清单4-4 自定义分区器实现
 *
 * 默认的分区器在 key 为 null 时不会选择非可用的分区 {@link DefaultPartitioner}
 * 这个分区器不会
 * props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
 *         DemoPartitioner.class.getName());
 */
public class DemoPartitioner implements Partitioner {

    private final AtomicInteger counter = new AtomicInteger(0);

    /**
     * 计算分区号，返回值为 int 类型
     *
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if (null == keyBytes) {
            return counter.getAndIncrement() % numPartitions;
        } else {
            return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        }
    }

    /**
     * 关闭分区器的时候用来回收一些资源
     */
    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
