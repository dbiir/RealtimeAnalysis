package cn.edu.ruc.realtime.partition;

import java.util.Random;

/**
 * Created by Jelly on 6/12/16.
 * Default fiber partitioner.
 */
public class LoaderClientPartitionDefault implements LoaderClientPartition {
    private static Random random = new Random();

    /**
     * Partition by provided key. Default partition num to 1000
     * @param key key to be partitioned
     * */
    public static int getPartition(String key) {
        return getPartition(key, 1000);
    }

    public static int getPartition(int key) {
        return getPartition(key, 1000);
    }

    public static long getPartition(long key) {
        return getPartition(key, 1000);
    }

    /**
     * Partition by key mod partition nums. Scale factor of partition num default to 1
     * @param key key to be partitioned
     * @param base base partition num
     * */
    public static int getPartition(String key, int base) {
        return getPartition(key, base, 1);
    }

    public static int getPartition(int key, int base) {
        return getPartition(key, base, 1);
    }

    public static long getPartition(long key, long base) {
        return getPartition(key, base, 1L);
    }

    /**
     * Partition by key mod partition nums.
     * @param key key to be partitioned
     * @param base base partition num
     * @param scaleFactor scale factor of partition num
     * */
    public static int getPartition(String key, int base, int scaleFactor) {
        int keyId;
        if (key == null) {
            keyId = random.nextInt(base);
        } else {
            keyId = key.hashCode();
        }
        return getPartition(keyId, base, scaleFactor);
    }

    public static int getPartition(int key, int base, int scaleFactor) {
        return key % (base * scaleFactor);
    }

    public static long getPartition(long key, long base, long scaleFactor) {
        return key % (base * scaleFactor);
    }
}
