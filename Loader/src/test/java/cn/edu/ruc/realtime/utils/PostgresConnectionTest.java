package cn.edu.ruc.realtime.utils;

import java.sql.Timestamp;

/**
 * RealTimeAnalysis
 *
 * @author Jelly
 */
public class PostgresConnectionTest {

    ConfigFactory configFactory = ConfigFactory.getInstance("/Users/Jelly/Developer/RealTimeAnalysis/config.props");

    public static void main(String[] args) {
        PostgresConnection conn = new PostgresConnection();
//        conn.execUpdate("CREATE TABLE metaTable (partition INT, file VARCHAR(300), beginTime TIMESTAMP, endTime TIMESTAMP);");
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        conn.commitMetaRecord(3, "tiss", timestamp, timestamp);
        conn.close();
    }
}
