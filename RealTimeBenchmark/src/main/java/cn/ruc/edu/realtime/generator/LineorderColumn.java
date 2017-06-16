package cn.ruc.edu.realtime.generator;

import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchColumnType;

import static io.airlift.tpch.GenerateUtils.formatDate;
import static io.airlift.tpch.TpchColumnTypes.IDENTIFIER;
import static io.airlift.tpch.TpchColumnTypes.DATE;
import static io.airlift.tpch.TpchColumnTypes.DOUBLE;
import static io.airlift.tpch.TpchColumnTypes.INTEGER;
import static io.airlift.tpch.TpchColumnTypes.varchar;

/**
 * RealTimeAnalysis
 *
 * @author guodong
 */
public enum LineorderColumn
    implements TpchColumn<Lineorder>
{
    @SuppressWarnings("SpellCheckingInspection")
    ORDER_KEY("l_orderkey", IDENTIFIER)
            {
                public long getIdentifier(Lineorder lineorder)
                {
                    return lineorder.getOrderKey();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    PART_KEY("l_partkey", IDENTIFIER)
            {
                public long getIdentifier(Lineorder lineorder)
                {
                    return lineorder.getPartKey();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    SUPPLIER_KEY("l_suppkey", IDENTIFIER)
            {
                public long getIdentifier(Lineorder lineorder)
                {
                    return lineorder.getSupplierKey();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    CUSTOMER_KEY("o_custkey", IDENTIFIER)
            {
                public long getIdentifier(Lineorder lineorder)
                {
                    return lineorder.getCustomerKey();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    ORDER_STATUS("o_orderstatus", varchar(1))
            {
                public String getString(Lineorder lineorder)
                {
                    return String.valueOf(lineorder.getOrderStatus());
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    TOTAL_PRICE("o_totalprice", DOUBLE)
            {
                public double getDouble(Lineorder lineorder)
                {
                    return lineorder.getTotalPrice();
                }

                public long getIdentifier(Lineorder lineorder)
                {
                    return lineorder.getTotalPriceInCents();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    ORDER_DATE("o_orderdate", DATE)
            {
                @Override
                public String getString(Lineorder lineorder)
                {
                    return formatDate(getDate(lineorder));
                }

                public int getDate(Lineorder lineorder)
                {
                    return lineorder.getOrderDate();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    ORDER_PRIORITY("o_orderpriority", varchar(15))
            {
                public String getString(Lineorder lineorder)
                {
                    return lineorder.getOrderPriority();
                }
            },

    CLERK("o_clerk", varchar(15))
            {
                public String getString(Lineorder lineorder)
                {
                    return lineorder.getClerk();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    SHIP_PRIORITY("o_shippriority", INTEGER)
            {
                public int getInteger(Lineorder lineorder)
                {
                    return lineorder.getShipPriority();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    ORDER_COMMENT("o_comment", varchar(79))
            {
                public String getString(Lineorder lineorder)
                {
                    return lineorder.getOrderComment();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    LINE_NUMBER("l_linenumber", INTEGER)
            {
                public int getInteger(Lineorder lineorder)
                {
                    return lineorder.getLineNumber();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    QUANTITY("l_quantity", DOUBLE)
            {
                public double getDouble(Lineorder lineorder)
                {
                    return lineorder.getQuantity();
                }

                public long getIdentifier(Lineorder lineorder)
                {
                    return lineorder.getQuantity() * 100;
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    EXTENDED_PRICE("l_extendedprice", DOUBLE)
            {
                public double getDouble(Lineorder lineorder)
                {
                    return lineorder.getExtendedPrice();
                }

                public long getIdentifier(Lineorder lineorder)
                {
                    return lineorder.getExtendedPriceInCents();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    DISCOUNT("l_discount", DOUBLE)
            {
                public double getDouble(Lineorder lineorder)
                {
                    return lineorder.getDiscount();
                }

                public long getIdentifier(Lineorder lineorder)
                {
                    return lineorder.getDiscountPercent();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    TAX("l_tax", DOUBLE)
            {
                public double getDouble(Lineorder lineorder)
                {
                    return lineorder.getTax();
                }

                public long getIdentifier(Lineorder lineorder)
                {
                    return lineorder.getTaxPercent();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    RETURN_FLAG("l_returnflag", varchar(1))
            {
                public String getString(Lineorder lineorder)
                {
                    return lineorder.getReturnFlag();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    LINE_STATUS("l_linestatus", varchar(1))
            {
                public String getString(Lineorder lineorder)
                {
                    return String.valueOf(lineorder.getLineStatus());
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    SHIP_DATE("l_shipdate", DATE)
            {
                public String getString(Lineorder lineorder)
                {
                    return formatDate(getDate(lineorder));
                }

                public int getDate(Lineorder lineorder)
                {
                    return lineorder.getShipDate();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    COMMIT_DATE("l_commitdate", DATE)
            {
                public String getString(Lineorder lineorder)
                {
                    return formatDate(getDate(lineorder));
                }

                public int getDate(Lineorder lineorder)
                {
                    return lineorder.getCommitDate();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    RECEIPT_DATE("l_receiptdate", DATE)
            {
                public String getString(Lineorder lineorder)
                {
                    return formatDate(getDate(lineorder));
                }

                @Override
                public int getDate(Lineorder lineorder)
                {
                    return lineorder.getReceiptDate();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    SHIP_INSTRUCTIONS("l_shipinstruct", varchar(25))
            {
                public String getString(Lineorder lineorder)
                {
                    return lineorder.getShipInstructions();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    SHIP_MODE("l_shipmode", varchar(10))
            {
                public String getString(Lineorder lineorder)
                {
                    return lineorder.getShipMode();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    LINEITEM_COMMENT("l_comment", varchar(44))
            {
                public String getString(Lineorder lineorder)
                {
                    return lineorder.getLineitemComment();
                }
            };

    private final String columnName;
    private final TpchColumnType type;

    LineorderColumn(String columnName, TpchColumnType type)
    {
        this.columnName = columnName;
        this.type = type;
    }

    @Override
    public String getColumnName()
    {
        return columnName;
    }

    @Override
    public TpchColumnType getType()
    {
        return type;
    }

    @Override
    public double getDouble(Lineorder lineorder)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getIdentifier(Lineorder lineorder)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInteger(Lineorder lineorder)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(Lineorder lineorder)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getDate(Lineorder lineorder)
    {
        throw new UnsupportedOperationException();
    }
}
