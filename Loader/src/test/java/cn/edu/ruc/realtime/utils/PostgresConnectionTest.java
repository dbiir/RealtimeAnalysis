package cn.edu.ruc.realtime.utils;

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
        long timestamp = System.currentTimeMillis();
        conn.commitMetaRecord(3, "tiss", timestamp, timestamp);
        conn.close();
    }
}
