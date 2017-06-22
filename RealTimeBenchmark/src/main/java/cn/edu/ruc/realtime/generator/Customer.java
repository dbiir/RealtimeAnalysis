package cn.edu.ruc.realtime.generator;

import static io.airlift.tpch.GenerateUtils.formatMoney;
import static java.util.Locale.ENGLISH;

/**
 * RealTimeAnalysis
 *
 * @author guodong
 */
public class Customer
{
    private final long rowNumber;
    private final long customerKey;
    private final String name;            // varchar(32)
    private final String address;         // varchar(40)
    private final long nationKey;
    private final String phone;           // varchar(15)
    private final long accountBalance;
    private final String marketSegment;   // varchar(10)
    private final String comment;         // varchar(117)

    public Customer(long rowNumber, long customerKey, String name, String address, long nationKey, String phone, long accountBalance, String marketSegment, String comment)
    {
        this.rowNumber = rowNumber;
        this.customerKey = customerKey;
        this.name = name;
        this.address = address;
        this.nationKey = nationKey;
        this.phone = phone;
        this.accountBalance = accountBalance;
        this.marketSegment = marketSegment;
        this.comment = comment;
    }

    public long getRowNumber()
    {
        return rowNumber;
    }

    public long getCustomerKey()
    {
        return customerKey;
    }

    public String getName()
    {
        return name;
    }

    public String getAddress()
    {
        return address;
    }

    public long getNationKey()
    {
        return nationKey;
    }

    public String getPhone()
    {
        return phone;
    }

    public long getAccountBalance()
    {
        return accountBalance;
    }

    public String getMarketSegment()
    {
        return marketSegment;
    }

    public String getComment()
    {
        return comment;
    }

    public String toLine()
    {
        return String.format(ENGLISH,
                "%d|%s|%s|%d|%s|%s|%s|%s",
                customerKey,
                name,
                address,
                nationKey,
                phone,
                formatMoney(accountBalance),
                marketSegment,
                comment);
    }
}
