package cn.edu.ruc.realtime.generator;

import io.airlift.tpch.TpchEntity;

/**
 * RealTimeAnalysis
 *
 * @author guodong
 */
public class Lineorder implements TpchEntity
{
    private final long rowNumber;
    private final long orderKey;
    private final long partKey;
    private final long supplierKey;
    private final long customerKey;
    private final char orderStatus;
    private final long totalPrice;
    private final int orderDate;
    private final String orderPriority;
    private final String clerk;
    private final int shipPriority;
    private final String orderComment;
    private final int lineNumber;
    private final long quantity;
    private final long extendedPrice;
    private final long discount;
    private final long tax;
    private final String returnFlag;
    private final char lineStatus;
    private final int shipDate;
    private final int commitDate;
    private final int receiptDate;
    private final String shipInstructions;
    private final String shipMode;
    private final String lineitemComment;
    private final long messageDate;

    public Lineorder(
            long rowNumber,
            long orderKey,
            long customerKey,
            char orderStatus,
            String clerk,
            String orderComment,
            String lineitemComment,
            long quantity,
            long discount,
            long tax,
            long partKey,
            long supplierKey,
            int shipDate,
            int commitDate,
            int receiptDate,
            String returnFlag,
            String shipInstructions,
            String shipMode,
            long totalPrice,
            int orderDate,
            String orderPriority,
            int shipPriority,
            int lineNumber,
            long extendedPrice,
            char lineStatus,
            long messageDate)
    {
        this.rowNumber = rowNumber;
        this.orderKey = orderKey;
        this.partKey = partKey;
        this.supplierKey = supplierKey;
        this.customerKey = customerKey;
        this.orderStatus = orderStatus;
        this.totalPrice = totalPrice;
        this.orderDate = orderDate;
        this.orderPriority = orderPriority;
        this.clerk = clerk;
        this.shipPriority = shipPriority;
        this.orderComment = orderComment;
        this.lineNumber = lineNumber;
        this.quantity = quantity;
        this.extendedPrice = extendedPrice;
        this.discount = discount;
        this.tax = tax;
        this.returnFlag = returnFlag;
        this.lineStatus = lineStatus;
        this.shipDate = shipDate;
        this.commitDate = commitDate;
        this.receiptDate = receiptDate;
        this.shipInstructions = shipInstructions;
        this.shipMode = shipMode;
        this.lineitemComment = lineitemComment;
        this.messageDate = messageDate;
    }

    public long getOrderKey()
    {
        return orderKey;
    }

    public long getPartKey()
    {
        return partKey;
    }

    public long getSupplierKey()
    {
        return supplierKey;
    }

    public long getCustomerKey()
    {
        return customerKey;
    }

    public char getOrderStatus()
    {
        return orderStatus;
    }

    public double getTotalPrice()
    {
        return totalPrice / 100.0;
    }

    public long getTotalPriceInCents()
    {
        return totalPrice;
    }

    public int getOrderDate()
    {
        return orderDate;
    }

    public String getOrderPriority()
    {
        return orderPriority;
    }

    public String getClerk()
    {
        return clerk;
    }

    public int getShipPriority()
    {
        return shipPriority;
    }

    public String getOrderComment()
    {
        return orderComment;
    }

    public int getLineNumber()
    {
        return lineNumber;
    }

    public long getQuantity()
    {
        return quantity;
    }

    public double getExtendedPrice()
    {
        return extendedPrice / 100.0;
    }

    public long getExtendedPriceInCents()
    {
        return extendedPrice;
    }

    public double getDiscount()
    {
        return discount / 100.0;
    }

    public long getDiscountPercent()
    {
        return discount;
    }

    public double getTax()
    {
        return tax / 100.0;
    }

    public long getTaxPercent()
    {
        return tax;
    }

    public String getReturnFlag()
    {
        return returnFlag;
    }

    public char getLineStatus()
    {
        return lineStatus;
    }

    public int getShipDate()
    {
        return shipDate;
    }

    public int getCommitDate()
    {
        return commitDate;
    }

    public int getReceiptDate()
    {
        return receiptDate;
    }

    public String getShipInstructions()
    {
        return shipInstructions;
    }

    public String getShipMode()
    {
        return shipMode;
    }

    public String getLineitemComment()
    {
        return lineitemComment;
    }

    public long getMessageDate()
    {
        return messageDate;
    }

    public long getRowNumber()
    {
        return rowNumber;
    }

    public String toLine()
    {
        return new StringBuilder()
                .append(customerKey)
                .append("|")
                .append(orderKey)
                .append("|")
                .append(partKey)
                .append("|")
                .append(supplierKey)
                .append("|")
                .append(lineNumber)
                .append("|")
                .append(quantity)
                .append("|")
                .append(extendedPrice)
                .append("|")
                .append(discount)
                .append("|")
                .append(tax)
                .append("|")
                .append(returnFlag)
                .append("|")
                .append(lineStatus)
                .append("|")
                .append(shipDate)
                .append("|")
                .append(commitDate)
                .append("|")
                .append(receiptDate)
                .append("|")
                .append(shipInstructions)
                .append("|")
                .append(shipMode)
                .append("|")
                .append(lineitemComment)
                .append("|")
                .append(orderStatus)
                .append("|")
                .append(totalPrice)
                .append("|")
                .append(orderDate)
                .append("|")
                .append(orderPriority)
                .append("|")
                .append(clerk)
                .append("|")
                .append(shipPriority)
                .append("|")
                .append(orderComment)
                .append("|")
                .append(messageDate)
                .toString();
    }
}
