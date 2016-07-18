package cn.edu.ruc.realtime.partition;

import java.util.Random;

/**
 * Created by Jelly on 6/12/16.
 * Default fiber partitioner.
 */
public class LoaderClientPartitionDefault implements LoaderClientPartition {
    /**
     * Partition by provided key. Default partition num to 1000
     * @param key key to be partitioned
     * */
    public static int getPartition(long key) {
        return getPartition(key, 1000);
    }

    /**
     * Partition by key mod partition nums. Scale factor of partition num default to 1
     * @param key key to be partitioned
     * @param base base partition num
     * */
    public static int getPartition(long key, long base) {
        return getPartition(key, base, 1L);
    }

    /**
     * Partition by key mod partition nums.
     * @param key key to be partitioned
     * @param base base partition num
     * @param scaleFactor scale factor of partition num
     * */
    public static int getPartition(long key, long base, long scaleFactor) {
        return (int) (key % (base * scaleFactor));
    }
}
