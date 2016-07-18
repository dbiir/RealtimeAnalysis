package cn.edu.ruc.realtime.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.SystemTime;

import java.util.Map;

/**
 * Created by Jelly on 6/12/16.
 * Kafka partitioner
 */
public class LoaderClientPartitionKafka implements Partitioner {
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int partitionNum = cluster.partitionsForTopic(topic).size();
        int result = LoaderClientPartitionDefault.getPartition((Long) key, partitionNum);
        return result;
    }

    public void close() {

    }

    public void configure(Map<String, ?> configs) {

    }
}
