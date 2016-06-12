package cn.edu.ruc.realtime.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * Created by Jelly on 6/12/16.
 * Default fiber partitioner. Suitable for Kafka producer
 */
public class LoaderPartitionDefault implements LoaderPartition, Partitioner {
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return 0;
    }

    // unused
    public int getPartition(String key) {
        return 0;
    }

    /**
     * Partition by key mod partition nums
     * */
    public int getPartition(String key, int option) {
        return 0;
    }

    public int getPartition(String key, int option, int scaleFactor) {
        return 0;
    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}
