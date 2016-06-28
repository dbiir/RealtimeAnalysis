package cn.edu.ruc.realtime.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * Created by Jelly on 6/12/16.
 * Kafka partitioner
 */
public class LoaderPartitionKafka implements Partitioner {
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int partitionNum = cluster.partitionsForTopic(topic).size();
        return LoaderPartitionDefault.getPartition((String) key, partitionNum);
    }

    public void close() {

    }

    public void configure(Map<String, ?> configs) {

    }
}
