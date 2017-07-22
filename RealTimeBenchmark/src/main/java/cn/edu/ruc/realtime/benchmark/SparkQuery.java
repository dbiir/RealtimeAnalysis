package cn.edu.ruc.realtime.benchmark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * RealTimeAnalysis
 *
 * @author guodong
 */
public class SparkQuery
{
    public static void main(String[] args) throws IOException
    {
        if (args.length != 2)
        {
            System.out.println("ARGS: queryFilePath, statPath");
            System.exit(-1);
        }
        String queryPath = args[0];
        String statPath = args[1];
        List<String> queries = new ArrayList<>(1000);
        List<String> statistics = new ArrayList<>(1000);
        BufferedReader reader = new BufferedReader(new FileReader(queryPath));
        String line = null;
        while ((line = reader.readLine()) != null)
        {
            queries.add(line.trim());
        }
        reader.close();
        SparkSession spark = SparkSession.builder()
                .appName("SparkQuery")
                .getOrCreate();

        long dfStart = System.currentTimeMillis();
        Dataset<Row> df = spark.read().parquet("/warehouse/test/realtime100");
        Dataset<Row> dfCustomer = spark.read().parquet("/warehouse/test/customer");
        df.createOrReplaceTempView("realtime100");
        dfCustomer.createOrReplaceTempView("customer");
        long dfEnd = System.currentTimeMillis();
        statistics.add("df time: " + (dfEnd - dfStart) + "ms");

        for (int i = 0; i < queries.size(); i++)
        {
            if (queries.get(i) != null)
            {
                try
                {
                    long queryStart = System.currentTimeMillis();
                    spark.sql(queries.get(i)).show();
                    long queryEnd = System.currentTimeMillis();
                    String stat = String.format(
                            "%d, %d, %s, %s",
                            i,
                            (queryEnd - queryStart),
                            queries.get(i),
                            "OK");
                    statistics.add(stat);
                    Thread.currentThread().sleep(500);
                }
                catch (Exception e)
                {
                    String stat = String.format(
                            "%d, %d, %s, %s",
                            i,
                            0,
                            queries.get(i),
                            e.getMessage());
                    e.printStackTrace();
                    statistics.add(stat);
                }
            }
        }

        writeOut(statPath, statistics);
    }

    private static void writeOut(String statPath, List<String> statistics)
    {
        try
        {
            System.out.println("Done with queries. Write out statistics");
            BufferedWriter writer = new BufferedWriter(new FileWriter(statPath, true));
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
}
