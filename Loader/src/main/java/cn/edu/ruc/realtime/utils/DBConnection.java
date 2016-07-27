package cn.edu.ruc.realtime.utils;

import java.sql.Timestamp;
import java.util.HashMap;

/**
 * Created by Jelly on 7/10/16.
 */
public abstract class DBConnection {

    public abstract void commitPartitionOffset(int partition, long offset);
    public abstract void commitPartitionOffsets(HashMap<Integer, Long> commitMap);
    public abstract void commitMetaRecord(int partition, String file, Timestamp begin, Timestamp end);
}
