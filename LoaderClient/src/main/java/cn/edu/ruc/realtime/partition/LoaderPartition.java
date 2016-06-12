package cn.edu.ruc.realtime.partition;

/**
 * Created by Jelly on 6/12/16.
 */
public interface LoaderPartition {

    /**
     * Get partition num by provided key
     * @param key string as partition key
     * */
    public int getPartition(String key);

    /**
     * Get partition num by provided key and default scale factor (1)
     * @param key string as partition key
     * @param option an optional int value providing optional info
     * */
    public int getPartition(String key, int option);

    /**
     * Get partition num by provided key and scale factor
     * @param key string as partition key
     * @param option an optional int value providing optional info
     * @param scaleFactor scale factor
     * */
    public int getPartition(String key, int option, int scaleFactor);
}
