package cn.edu.ruc.realtime.utils;

import java.sql.*;
import java.util.HashMap;
import java.util.Properties;

/**
 * Created by Jelly on 7/10/16.
 */
public class PostgresConnection extends DBConnection {

    private ConfigFactory configFactory = ConfigFactory.getInstance();
    private String url = configFactory.getDBConnectionURL();
    private String user = configFactory.getDBConnectionUser();
    private String pass = configFactory.getDBConnectionPwd();
    private String driverClass = configFactory.getDBConnectionDriverClass();
    private String tableN = configFactory.getMetaTableName();
    private static Connection conn;

    public PostgresConnection() {
        Properties props = new Properties();
        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        props.setProperty("user", user);
        props.setProperty("password", pass);
        try {
            if (conn == null || conn.isClosed())
                conn = DriverManager.getConnection(url, props);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public ResultSet execQuery(String query) {
        Statement statement;
        try {
            statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(query);
            return resultSet;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void execUpdate(String query) {
        Statement statement;
        try {
            statement = conn.createStatement();
            statement.executeUpdate(query);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void commitPartitionOffset(int partition, long offset) {
//        StringBuffer sb = new StringBuffer();
        // if exists update else insert
//        sb.append("INSERT INTO offset VALUES(").append(partition).append(", ").append(offset).append(");");
    }

    @Override
    public void commitPartitionOffsets(HashMap<Integer, Long> commitMap) {
//        StringBuffer sb = new StringBuffer();
        // if exists update else insert
    }

    /**
     * INSERT INTO metatable VALUES(partition, 'file', 'begin', 'end');
     * */
    public void commitMetaRecord(long partition, String file, long beginTime, long endTime) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ")
                .append(tableN)
                .append(" VALUES('")
                .append(configFactory.getWriterDBName())
                .append("', '")
                .append(configFactory.getWriterTableName())
                .append("', ")
                .append(partition)
                .append(", ")
                .append(beginTime)
                .append(", ")
                .append(endTime)
                .append(", '")
                .append(file)
                .append("');");
        execUpdate(sb.toString());
    }

    public void close() {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
