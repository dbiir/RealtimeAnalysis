package cn.edu.ruc.realtime;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * RealTimeAnalysis
 *
 * @author guodong
 */
public class PreProcess
{
    @Test
    public void processSparkTimePointQueries() throws IOException, ParseException
    {
        BufferedReader reader = new BufferedReader(new FileReader("/Users/Jelly/Developer/RealTimeAnalysis/performance/Query-TimePoint"));
        BufferedWriter writer = new BufferedWriter(new FileWriter("/Users/Jelly/Developer/RealTimeAnalysis/performance/sparkQ0"));

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String line = null;
        while ((line = reader.readLine()) != null)
        {
            String originQ = line.split(":\\s")[1].trim();
            String[] parts = originQ.split("'");
            String smallT = parts[1];
            String largeT = parts[3];
            long st = sdf.parse(smallT).getTime();
            long lt = sdf.parse(largeT).getTime();
            writer.write("SELECT SUM(quantity) AS sum_qty , AVG(extendedprice) AS avg_price, avg(discount) AS avg_disc, count(*) AS count_order, min(orderkey) AS min_orderkey, max(orderkey) AS max_orderkey FROM realtime100 WHERE messagedate>" + st + " and messagedate<" + lt);
            writer.newLine();
        }
        writer.flush();
        reader.close();
        writer.close();
    }

    @Test
    public void processSparkCustKeyQueries() throws IOException
    {
        BufferedReader reader = new BufferedReader(new FileReader("/Users/Jelly/Developer/RealTimeAnalysis/performance/Query-CustKey"));
        BufferedWriter writer = new BufferedWriter(new FileWriter("/Users/Jelly/Developer/RealTimeAnalysis/performance/sparkQ1"));

        String line = null;
        while ((line = reader.readLine()) != null)
        {
            String q = line.split(":\\s")[1].trim();
            writer.write(q);
            writer.newLine();
        }
        writer.flush();
        reader.close();
        writer.close();
    }

    @Test
    public void processSparkCustKeyTimePointQueries() throws IOException, ParseException
    {
        BufferedReader reader = new BufferedReader(new FileReader("/Users/Jelly/Developer/RealTimeAnalysis/performance/Query-CustKeyTimePoint"));
        BufferedWriter writer = new BufferedWriter(new FileWriter("/Users/Jelly/Developer/RealTimeAnalysis/performance/sparkQ2"));

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String line = null;
        while ((line = reader.readLine()) != null)
        {
            String originQ = line.split(":\\s")[1].trim();
            String[] parts = originQ.split("'");
            String smallT = parts[1];
            String largeT = parts[3];
            long st = sdf.parse(smallT).getTime();
            long lt = sdf.parse(largeT).getTime();
            writer.write("SELECT SUM(quantity) AS sum_qty , AVG(extendedprice) AS avg_price, avg(discount) AS avg_disc, count(*) AS count_order, min(orderkey) AS min_orderkey, max(orderkey) AS max_orderkey FROM realtime100 WHERE messagedate>" + st + " and messagedate<" + lt + parts[4]);
            writer.newLine();
        }
        writer.flush();
        reader.close();
        writer.close();
    }

    @Test
    public void processSparkCustKeyJoinQueries() throws IOException
    {
        BufferedReader reader = new BufferedReader(new FileReader("/Users/Jelly/Developer/RealTimeAnalysis/performance/Spark-CustJoin"));
        BufferedWriter writer = new BufferedWriter(new FileWriter("/Users/Jelly/Developer/RealTimeAnalysis/performance/sparkQ3"));

        String line = null;
        while ((line = reader.readLine()) != null)
        {
            writer.write(line.split(":\\s")[1].trim());
            writer.newLine();
        }
        writer.flush();
        reader.close();
        writer.close();
    }

    @Test
    public void processSparkTimePointJoinQueries() throws IOException, ParseException
    {
        BufferedReader reader = new BufferedReader(new FileReader("/Users/Jelly/Developer/RealTimeAnalysis/performance/Spark-TimePointJoin"));
        BufferedWriter writer = new BufferedWriter(new FileWriter("/Users/Jelly/Developer/RealTimeAnalysis/performance/sparkQ4"));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        String line = null;
        while ((line = reader.readLine()) != null)
        {
            String originQ = line.split(":\\s")[1];
            String[] parts = originQ.split("'");
            String smallT = parts[1];
            String largeT = parts[3];
            long st = sdf.parse(smallT).getTime();
            long lt = sdf.parse(largeT).getTime();
            writer.write("SELECT count(*) AS num FROM realtime100, customer WHERE messagedate>" + st + " and messagedate<" + lt + parts[4]);
            writer.newLine();
        }
        writer.flush();
        reader.close();
        writer.close();

    }
}
