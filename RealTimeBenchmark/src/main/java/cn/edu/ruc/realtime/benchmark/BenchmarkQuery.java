package cn.edu.ruc.realtime.benchmark;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
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
//    private static final long TOTAL_COUNT = 472000000L;
    private static final long TOTAL_COUNT = 1000000L;

    public static void main(String[] args)
    {
        if (args.length < 4)
        {
            System.out.println("ARGS: <repetitionTimes> <sleepSeconds> <queryMode> <statisticFilePath> [dataFilePath or timePointsPath,custKeysPath]");
            System.exit(-1);
        }
        int times = Integer.parseInt(args[0]);
        int sleepSecs = Integer.parseInt(args[1]);
        int mode = Integer.parseInt(args[2]);
        String statPath = args[3];
        String dataFilePath = "";
        if (args.length == 5)
        {
            dataFilePath = args[4];
        }
        System.out.println("Times: " + times + ", Sleep: " + sleepSecs + ", Mode: " + mode + ", StatPath: " + statPath + ", DataFilePath: " + dataFilePath);
        BenchmarkQuery benchmarkQuery = new BenchmarkQuery();
        if (mode == 1)
        {
            benchmarkQuery.testTimestamp(times, sleepSecs, statPath);
        }
        if (mode == 2)
        {
            benchmarkQuery.testCustkey(times, sleepSecs, statPath, dataFilePath);
        }
        if (mode == 4)
        {
            benchmarkQuery.testTimestampAndCustkey(times, sleepSecs, statPath, dataFilePath);
        }
        if (mode == 3) // 1 + 2
        {
            benchmarkQuery.testTimestamp(times, sleepSecs, statPath);
            benchmarkQuery.testCustkey(times, sleepSecs, statPath, dataFilePath);
        }
        if (mode == 5) // 1 + 4
        {
            benchmarkQuery.testTimestamp(times, sleepSecs, statPath);
            benchmarkQuery.testTimestampAndCustkey(times, sleepSecs, statPath, dataFilePath);
        }
        if (mode == 6) // 2 + 4
        {
            benchmarkQuery.testCustkey(times, sleepSecs, statPath, dataFilePath);
            benchmarkQuery.testTimestampAndCustkey(times, sleepSecs, statPath, dataFilePath);
        }
        if (mode == 7) // 1 + 2 + 4
        {
            benchmarkQuery.testTimestamp(times, sleepSecs, statPath);
            benchmarkQuery.testCustkey(times, sleepSecs, statPath, dataFilePath);
            benchmarkQuery.testTimestampAndCustkey(times, sleepSecs, statPath, dataFilePath);
        }
        if (mode == 8)  // JOIN
        {
            String[] parts = dataFilePath.split(",");
            String timePointsPath = parts[0].trim();
            String custKeysPath = parts[1].trim();
            benchmarkQuery.testJoin(times, sleepSecs, statPath, timePointsPath, custKeysPath);
        }
    }

    private void testTimestamp(int times, int sleepSecs, String statPath)
    {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
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
                    Thread.sleep(sleepSecs * 100);
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

    private void testCustkey(int times, int sleepSecs, String statPath, String dataFilePath)
    {
        List<String> statistics = new ArrayList<>(times);

        Connection conn = null;
        try
        {
            Properties properties = new Properties();
            properties.put("user", "presto");
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(DB_URL, properties);

            int baseCap = 100000;
            String[] custKeyBaseArr = genCustKey(baseCap, dataFilePath);

            String custK = "";
            for (int i = 0; i < times; i++)
            {
                try
                {
                    Statement stmt = conn.createStatement();
                    int rnd = ThreadLocalRandom.current().nextInt(custKeyBaseArr.length);
                    custK = custKeyBaseArr[rnd];
                    String sql = String.format(
                            "SELECT SUM(quantity) AS sum_qty , AVG(extendedprice) AS avg_price, avg(discount) AS avg_disc, count(*) AS count_order, min(orderkey) AS min_orderkey, max(orderkey) AS max_orderkey FROM realtime100 WHERE custkey=%s",
                            custK
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
                    // queryId, timeCost, resultCount, customerKey, executionState
                    String stat = String.format(
                            "QUERY%d, %d, %d, %s, %s",
                            i,
                            (end - start),
                            resultCount,
                            custK,
                            "OK");
                    statistics.add(stat);
                    System.out.println("QUERY" + i + ": " + sql);
                    Thread.sleep(sleepSecs * 100);
                }
                catch (Exception e)
                {
                    String stat = String.format(
                            "QUERY%d, %d, %d, %s, %s",
                            i,
                            0,
                            0,
                            custK,
                            e.getMessage());
                    e.printStackTrace();
                    statistics.add(stat);
                }
            }
        }
        catch (ClassNotFoundException | SQLException e)
        {
            e.printStackTrace();
        } finally
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

        // write out statistics
        writeOut(times, statPath, statistics);
    }

    private void testTimestampAndCustkey(int times, int sleepSecs, String statPath, String dataFilePath)
    {
        List<String> statistics = new ArrayList<>(times);

        int baseCap = 100000;
        String[] baseCustKeyArr = genCustKey(baseCap, dataFilePath);

        String[] timePoints = new String[times];
        long[] timeRanges = new long[times];
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
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
            String custK = "";

            for (int i = 0; i <times; i++)
            {
                try
                {
                    Statement stmt = conn.createStatement();
                    int rnd = ThreadLocalRandom.current().nextInt(baseCustKeyArr.length);
                    custK = baseCustKeyArr[rnd];
                    String sql = String.format(
                            "SELECT SUM(quantity) AS sum_qty , AVG(extendedprice) AS avg_price, avg(discount) AS avg_disc, count(*) AS count_order, min(orderkey) AS min_orderkey, max(orderkey) AS max_orderkey FROM realtime100 WHERE messagedate>timestamp '%s' and messagedate<timestamp '%s' and custkey=%s",
                            timePoints[i],
                            TIMESTAMP_MAX,
                            custK
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
                    // queryId, timeCost, resultCount, customerKey, timePoint, timeRange, executionState
                    String stat = String.format(
                            "QUERY%d, %d, %d, %s, %s, %d, %s",
                            i,
                            (end - start),
                            resultCount,
                            custK,
                            timePoints[i],
                            timeRanges[i],
                            "OK");
                    statistics.add(stat);
                    System.out.println("QUERY" + i + ": " + sql);
                    Thread.sleep(sleepSecs * 100);
                } catch (Exception e)
                {
                    String stat = String.format(
                            "QUERY%d, %d, %d, %s, %s, %d, %s",
                            i,
                            0,
                            0,
                            custK,
                            timePoints[i],
                            timeRanges[i],
                            e.getMessage());
                    e.printStackTrace();
                    statistics.add(stat);
                }
            }
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

        // write out statistics
        writeOut(times, statPath, statistics);
    }

    private void testJoin(int times, int sleepSecs, String statPath, String timePointsPath, String custKeysPath)
    {
        List<String> statistics = new ArrayList<>(times * 2);

        Connection conn = null;
        try (BufferedReader timePointsReader = new BufferedReader(new FileReader(timePointsPath));
             BufferedReader custKeysReader = new BufferedReader(new FileReader(custKeysPath)))
        {
            Properties properties = new Properties();
            properties.put("user", "presto");
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(DB_URL, properties);
            String line = null;
            int index = 0;

            // custKey join
            while ((line = custKeysReader.readLine()) != null && index < times)
            {
                try
                {
                    Statement stmt = conn.createStatement();
                    String sql = String.format(
                            "SELECT quantity , extendedprice, discount, orderkey, ordercomment, name, nationkey, phone FROM realtime100, customer WHERE custkey=%s and customerkey=custkey",
                            line);
                    long resultCount = 0L;
                    long start = System.currentTimeMillis();
                    ResultSet resultSet = stmt.executeQuery(sql);
                    while (resultSet.next())
                    {
                        resultCount++;
                    }
                    long end = System.currentTimeMillis();
                    index++;
                    resultSet.close();
                    stmt.close();
                    // queryId, timeCost, resultCount, customerKey, executionState
                    String stat = String.format(
                            "QUERY-CUSTKEY-JOIN%d, %d, %d, %s, %s",
                            index,
                            (end - start),
                            resultCount,
                            line,
                            "OK");
                    statistics.add(stat);
                    System.out.println("QUERY" + index + ": " + sql);
                    Thread.sleep(sleepSecs * 100);
                }
                catch (Exception e)
                {
                    String stat = String.format(
                            "QUERY-CUSTKEY-JOIN%d, %d, %d, %s, %s",
                            index,
                            0,
                            0,
                            line,
                            e.getMessage());
                    e.printStackTrace();
                    statistics.add(stat);
                }
            }
            System.out.println("Done with custkey join. Current timestamp: " + new Timestamp(System.currentTimeMillis()));

            index = 0;
            // time point join
            while ((line = timePointsReader.readLine()) != null && index < times)
            {
                try
                {
                    Statement stmt = conn.createStatement();
                    String sql = String.format(
                            "SELECT count(*) AS num FROM realtime100, customer WHERE messagedate>timestamp '%s' and messagedate<timestamp '%s' and customerkey=custkey",
                            line,
                            TIMESTAMP_MAX);
                    long start = System.currentTimeMillis();
                    long resultCount = 0L;
                    long end = 0L;
                    ResultSet resultSet = stmt.executeQuery(sql);
                    if (resultSet.next())
                    {
                        resultCount = resultSet.getLong("num");
                        end = System.currentTimeMillis();
                    }
                    index++;
                    resultSet.close();
                    stmt.close();
                    // queryId, timeCost, resultCount, timePoint, executionState
                    String stat = String.format(
                            "QUERY-TIMEPOINT-JOIN%d, %d, %d, %s, %s",
                            index,
                            (end - start),
                            resultCount,
                            line,
                            "OK");
                    statistics.add(stat);
                    System.out.println("QUERY" + index + ": " + sql);
                    Thread.sleep(sleepSecs * 100);
                }
                catch (Exception e)
                {
                    String stat = String.format(
                            "QUERY-TIMEPOINT-JOIN%d, %d, %d, %s, %s",
                            index,
                            0,
                            0,
                            line,
                            e.getMessage());
                    e.printStackTrace();
                    statistics.add(stat);
                }
            }
            System.out.println("Done with time point join. Current timestamp: " + new Timestamp(System.currentTimeMillis()));
        }
        catch (ClassNotFoundException | SQLException | IOException e)
        {
            e.printStackTrace();
        } finally
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

        // write out statistics
        writeOut(times, statPath, statistics);
    }

    private void writeOut(int times, String statPath, List<String> statistics)
    {
        try
        {
            System.out.println("Done with queries. Write out statistics");
            BufferedWriter writer = new BufferedWriter(new FileWriter(statPath, true));
            writer.write("Done with benching. Current timestamp: " + new Timestamp(System.currentTimeMillis()));
            for (String stat : statistics)
            {
                writer.write(stat);
                writer.newLine();
            }
            writer.flush();
            writer.close();
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private String[] genCustKey(int baseCap, String dataFilePath)
    {
        int scale = (int) (TOTAL_COUNT / baseCap);
        Set<String> custKeyBase = new HashSet<>(baseCap);
        int baseIndex = 0;
        String[] paths = dataFilePath.split(",");
        for (String path : paths)
        {
            try
            {
                BufferedReader reader = new BufferedReader(new FileReader(path.trim()));
                String line = null;
                while ((line = reader.readLine()) != null)
                {
                    long random = ThreadLocalRandom.current().nextLong(0, TOTAL_COUNT);
                    if ((baseIndex < baseCap) && (random < scale))
                    {
                        custKeyBase.add(line.split("\\|")[0]);
                        baseIndex = custKeyBase.size();
                    }
                }
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        }
        System.out.println("Done randomly generate customer keys. Current timestamp: " + new Timestamp(System.currentTimeMillis()));
        return custKeyBase.toArray(new String[0]);
    }
}
