package cn.edu.ruc.realtime.benchmark;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * RealTimeAnalysis
 *
 * @author guodong
 */
public class BenchmarkQuery
{
    private static final String JDBC_DRIVER = "com.facebook.presto.jdbc.PrestoDriver";
    private static final String DB_URL = "jdbc:presto://10.77.50.165:8080/hdfs/test";
    private static final long UNIXTIME_MIN = 1498026399288L;
    private static final long UNIXTIME_MAX = 1498028851424L;
    private static final String TIMESTAMP_MAX = "2017-06-21 15:07:31.424";
    private static final long TOTAL_COUNT = 472000000L;

    public static void main(String[] args)
    {
        if (args.length < 4)
        {
            System.out.println("ARGS: <repetitionTimes> <sleepSeconds> <queryMode> <statisticFilePath> [dataFilePath]");
            System.exit(-1);
        }
        int times = Integer.parseInt(args[0]);
        int sleepSecs = Integer.parseInt(args[1]);
        int mode = Integer.parseInt(args[2]);
        String statPath = args[3];
        System.out.println("Times: " + times + ", Sleep: " + sleepSecs + ", Mode: " + mode + ", StatPath: " + statPath);
        BenchmarkQuery benchmarkQuery = new BenchmarkQuery();
        if (mode == 0)
        {
            benchmarkQuery.testTimestamp(times, sleepSecs, statPath);
        }
        if (mode == 1)
        {
            benchmarkQuery.testCustkey(times, sleepSecs, statPath);
        }
        if (mode == 2)
        {
            //benchmarkQuery.testTimestampAndCustkey();
        }
    }

    private void testTimestamp(int times, int sleepSecs, String statPath)
    {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<String> statistics = new ArrayList<>(times);

        String[] timePoints = new String[times];
        long[] timeRanges = new long[times];
        for (int i = 0; i < times; i++)
        {
            long rndT = ThreadLocalRandom.current().nextLong(UNIXTIME_MIN, UNIXTIME_MAX);
            Date date = new Date(rndT);
            timePoints[i] = sdf.format(date);
            timeRanges[i] = rndT - UNIXTIME_MIN;
        }

        Connection conn = null;
        try
        {
            Properties properties = new Properties();
            properties.put("user", "presto");
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(DB_URL, properties);

            for (int i = 0; i < times; i++)
            {
                try
                {
                    Statement stmt = conn.createStatement();
                    String sql = String.format(
                            "SELECT SUM(quantity) AS sum_qty , AVG(extendedprice) AS avg_price, avg(discount) AS avg_disc, count(*) AS count_order, min(orderkey) AS min_orderkey, max(orderkey) AS max_orderkey FROM realtime100 WHERE messagedate>timestamp '%s' and messagedate<timestamp '%s'",
                            timePoints[i],
                            TIMESTAMP_MAX
                    );
                    long resultCount = 0L;
                    long start = System.currentTimeMillis();
                    long end = 0L;
                    ResultSet resultSet = stmt.executeQuery(sql);
                    if (resultSet.next())
                    {
                        end = System.currentTimeMillis();
                        resultCount = resultSet.getLong("count_order");
                    }
                    resultSet.close();
                    stmt.close();
                    // queryId, timeCost, resultCount, timePoint, timeRange, executionState
                    String stat = String.format(
                            "QUERY%d, %d, %d, %s, %d, %s",
                            i,
                            (end - start),
                            resultCount,
                            timePoints[i],
                            timeRanges[i],
                            "OK");
                    statistics.add(stat);
                    System.out.println("QUERY" + i + ": " + sql);
                    Thread.sleep(sleepSecs * 1000);
                }
                catch (Exception e)
                {
                    String stat = String.format(
                            "QUERY%d, %d, %d, %s, %d, %s",
                            i,
                            0,
                            0,
                            timePoints[i],
                            timeRanges[i],
                            e.getMessage());
                    e.printStackTrace();
                    statistics.add(stat);
                }
            }

            writeOut(times, statPath, statistics);
        }
        catch (ClassNotFoundException | SQLException e)
        {
            e.printStackTrace();
        }
        finally
        {
            if (conn != null)
            {
                try
                {
                    conn.close();
                } catch (SQLException e)
                {
                    e.printStackTrace();
                }
            }
        }
    }

    private void testCustkey(int times, int sleepSecs, String statPath)
    {
        List<String> statistics = new ArrayList<>(times);
        int base = 100 * 1000;
        String[] custKeyBase = new String[base];
        Connection conn = null;
        try
        {
            Properties properties = new Properties();
            properties.put("user", "presto");
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(DB_URL, properties);
        } catch (ClassNotFoundException | SQLException e)
        {
            e.printStackTrace();
        }
        writeOut(times, statPath, statistics);
    }

    private void testTimestampAndCustkey(int times, int sleepSecs, String statPath)
    {
        List<String> statistics = new ArrayList<>(times);

        writeOut(times, statPath, statistics);
    }

    private void writeOut(int times, String statPath, List<String> statistics)
    {
        try
        {
            System.out.println("Done with queries. Write out statistics");
            BufferedWriter writer = new BufferedWriter(new FileWriter(statPath));
            for (int i = 0; i < times; i++)
            {
                writer.write(statistics.get(i));
                writer.newLine();
            }
            writer.flush();
            writer.close();
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
