package cn.edu.ruc.realtime.partition;

/**
 * Created by Jelly on 6/12/16.
 */
public interface LoaderClientPartition {

    /**
     * Get partition num by provided key
     * @param key string as partition key
     * */
    static int getPartition(String key) { return 0; }

    /**
     * Get partition num by provided key and default scale factor (1)
     * @param key string as partition key
     * @param option an optional int value providing optional info
     * */
    static int getPartition(String key, int option) { return 0; }

    /**
     * Get partition num by provided key and scale factor
     * @param key string as partition key
     * @param option an optional int value providing optional info
     * @param scaleFactor scale factor
     * */
    static int getPartition(String key, int option, int scaleFactor) { return 0; }
}
