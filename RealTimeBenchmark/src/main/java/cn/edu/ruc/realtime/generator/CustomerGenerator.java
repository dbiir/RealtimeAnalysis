package cn.edu.ruc.realtime.generator;

import io.airlift.tpch.Distributions;
import io.airlift.tpch.RandomAlphaNumeric;
import io.airlift.tpch.RandomBoundedInt;
import io.airlift.tpch.RandomPhoneNumber;
import io.airlift.tpch.RandomString;
import io.airlift.tpch.RandomText;
import io.airlift.tpch.TextPool;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Properties;

/**
 * RealTimeAnalysis
 *
 * @author guodong
 */
public class CustomerGenerator
{
    private static final int ADDRESS_AVERAGE_LENGTH = 25;
    private static final int ACCOUNT_BALANCE_MIN = -99999;
    private static final int ACCOUNT_BALANCE_MAX = 999999;
    private static final int COMMENT_AVERAGE_LENGTH = 73;
    private static final String JDBC_DRIVER = "com.facebook.presto.jdbc.PrestoDriver";
    private final RandomAlphaNumeric addressRandom = new RandomAlphaNumeric(881155353, ADDRESS_AVERAGE_LENGTH);
    private final RandomBoundedInt nationKeyRandom;
    private final RandomPhoneNumber phoneRandom = new RandomPhoneNumber(1521138112);
    private final RandomBoundedInt accountBalanceRandom = new RandomBoundedInt(298370230, ACCOUNT_BALANCE_MIN, ACCOUNT_BALANCE_MAX);
    private final RandomString marketSegmentRandom;
    private final RandomText commentRandom;

    private long index;

    public static void main(String[] args)
    {
        String dbUrl = null;
        if (args.length < 1 || args.length > 2)
        {
            System.out.println("ARGS: <customerPath> [dbUrl]");
            System.exit(-1);
        }
        if (args.length == 1)
        {
            dbUrl = "jdbc:presto://10.77.50.165:8080/hdfs/test";
        }
        if (args.length == 2)
        {
            dbUrl = args[1];
        }
        String customerPath = args[0];
        CustomerGenerator customerGenerator = new CustomerGenerator(Distributions.getDefaultDistributions(), TextPool.getDefaultTestPool(), 0L);
        customerGenerator.generate(dbUrl, customerPath);
    }

    private CustomerGenerator(Distributions distributions, TextPool textPool, long startIndex)
    {
        nationKeyRandom = new RandomBoundedInt(1489529863, 0 , distributions.getNations().size() - 1);
        marketSegmentRandom = new RandomString(1140279430, distributions.getMarketSegments());
        commentRandom = new RandomText(1335826707, textPool, COMMENT_AVERAGE_LENGTH);

        addressRandom.advanceRows(startIndex);
        nationKeyRandom.advanceRows(startIndex);
        phoneRandom.advanceRows(startIndex);
        accountBalanceRandom.advanceRows(startIndex);
        marketSegmentRandom.advanceRows(startIndex);
        commentRandom.advanceRows(startIndex);
    }

    private void generate(String dbUrl, String customerPath)
    {
        try(BufferedWriter writer = new BufferedWriter(new FileWriter(customerPath)))
        {
            Connection conn = null;
            try
            {
                Properties properties = new Properties();
                properties.put("user", "presto");
                Class.forName(JDBC_DRIVER);
                conn = DriverManager.getConnection(dbUrl, properties);
                Statement stmt = conn.createStatement();
                String sql = "SELECT DISTINCT custkey FROM realtime100";
                ResultSet resultSet = stmt.executeQuery(sql);
                while (resultSet.next())
                {
                    long custKey = resultSet.getLong(0);
                    Customer customer = computeNext(custKey);
                    writer.write(customer.toLine());
                    writer.newLine();
                }
                resultSet.close();
                stmt.close();
                writer.flush();
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
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private Customer computeNext(long customerKey)
    {
        Customer customer = makeCustomer(index, customerKey);

        addressRandom.rowFinished();
        nationKeyRandom.rowFinished();
        phoneRandom.rowFinished();
        accountBalanceRandom.rowFinished();
        marketSegmentRandom.rowFinished();
        commentRandom.rowFinished();

        index++;

        return customer;
    }

    private Customer makeCustomer(long rowCount, long customerKey)
    {
        long nationKey = nationKeyRandom.nextValue();

        return new Customer(
                rowCount,
                customerKey,
                String.format(Locale.ENGLISH, "Customer#%09d", customerKey),
                addressRandom.nextValue(),
                nationKey,
                phoneRandom.nextValue(nationKey),
                accountBalanceRandom.nextValue(),
                marketSegmentRandom.nextValue(),
                commentRandom.nextValue());
    }
}
