package cn.ruc.edu.realtime.benchmark;

import cn.edu.ruc.realtime.utils.ConfigFactory;
import cn.edu.ruc.realtime.utils.DBConnection;
import cn.edu.ruc.realtime.utils.PostgresConnection;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.collection.mutable.StringBuilder;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * RealTimeAnalysis
 *
 * TODO change datetime in parquet schema to int96
 * @author Jelly
 */
public class QueryBenchV1 {
    private static SparkConf sparkConf = new SparkConf()
            .setAppName("QueryBenchV1")
            .setMaster("spark://scujgd01:7077")
            .set("spark.sql.parquet.binaryAsString", "true");
    private static JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    private static SQLContext sqlContext = new SQLContext(ctx);
    private static ConfigFactory configFactory = ConfigFactory.getInstance("/home/es/config.props");
    private static DBConnection dbConnection = new PostgresConnection();

    public static void main(String[] args) {
//        if (args.length != 2) {
//            System.out.println("metaSQL querySQL ...");
//            System.exit(-1);
//        }
//        List<Integer> partitions = new ArrayList<Integer>();
//        String beginTime = args[0];
//        String endTime = args[1];
//        for (int i = 2; i < args.length; i++) {
//            partitions.add(Integer.parseInt(args[i]));
//        }
        String metaSQL = configFactory.getTestMetaSQL();
        String querySQL = configFactory.getTestQuerySQL();
        long before = System.currentTimeMillis();
        Set<String> meta = queryMeta(metaSQL);
        for (String m: meta) {
            System.out.println("File: " + m);
        }
        System.out.println("File Size: " + meta.size());

//        StringBuilder sb = new StringBuilder();
//        sb.append("select sum(quantity) as sum_qty, sum(extendedprice) as sum_base_price, " +
//                "avg(quantity) as avg_qty, avg(extendedprice) as avg_price, avg(discount) as avg_disc, " +
//                "count(*) as count_order, min(orderkey) as min_orderkey, max(orderkey) as max_orderkey from realtime");
//        boolean condition = false;
//        if (!beginTime.equalsIgnoreCase("BEGIN")) {
//            condition = true;
//            String[] parts = beginTime.split("-");
//            beginTime = parts[0]+"-"+parts[1]+"-"+parts[2]+" "+parts[3];
//            sb.append(" WHERE messagedate >= '" + beginTime + "'");
//        }
//        if (!endTime.equalsIgnoreCase("END")) {
//            String[] parts = endTime.split("-");
//            endTime = parts[0]+"-"+parts[1]+"-"+parts[2]+" "+parts[3];
//            if (!condition) {
//                sb.append(" WHERE");
//            } else {
//                sb.append(" AND");
//            }
//            sb.append(" messagedate <= '" + endTime + "'");
//        }

//        queryParquet(meta, querySQL);
        queryText(meta, querySQL);
        long end = System.currentTimeMillis();
        System.out.println("Done query, cost: " + (end - before) + " ms.");
    }

    public static void queryParquet(Set<String> files, String sql) {
        Seq<String> seq = JavaConversions.asScalaSet(files).toSeq();
        DataFrame df = sqlContext.read().parquet(seq);
        df.registerTempTable("realtime");
        df.printSchema();
        DataFrame result = sqlContext.sql(sql);
        result.show();
        System.out.println("Count: " + df.count() + ", Result: " + result.count());
    }

    public static void queryText(Set<String> files, String sql) {
        StringBuilder sb = new StringBuilder();
        String[] fileA = files.toArray(new String[]{});

        for (int i = 0; i < fileA.length-1; i++) {
            sb.append(fileA[i]).append(",");
        }
        sb.append(fileA[fileA.length-1]);
        System.out.println(sb.toString());
        JavaRDD<RealTime> fRDD = ctx.textFile(sb.toString()).map(
                new Function<String, RealTime>() {
                    public RealTime call(String line) throws Exception {
                        String[] parts = line.split("\\|");
//                        System.out.println(line.split(":")[1]);
                        if (parts.length != 25) {
                            System.out.println("LINE: " + line);
                            return new RealTime();
                        }
                        RealTime rt = new RealTime();
                        rt.setCustkey(Integer.parseInt(parts[0].trim().split(":")[1].trim()));
                        rt.setOrderkey(Integer.parseInt(parts[1].trim()));
                        rt.setPartkey(Integer.parseInt(parts[2].trim()));
                        rt.setSuppkey(Integer.parseInt(parts[3].trim()));
                        rt.setLinenumber(Integer.parseInt(parts[4].trim()));
                        rt.setQuantity(Float.parseFloat(parts[5].trim()));
                        rt.setExtendedprice(Float.parseFloat(parts[6].trim()));
                        rt.setDiscount(Float.parseFloat(parts[7].trim()));
                        rt.setTax(Float.parseFloat(parts[8].trim()));
                        rt.setReturnflag(parts[9].trim());
                        rt.setLinestatus(parts[10].trim());
                        rt.setShipdate(parts[11].trim());
                        rt.setCommitdate(parts[12].trim());
                        rt.setReceiptdate(parts[13].trim());
                        rt.setShipinstruct(parts[14].trim());
                        rt.setShipmode(parts[15].trim());
                        rt.setComment(parts[16].trim());
                        rt.setOrderstatus(parts[17].trim());
                        rt.setTotalprice(Float.parseFloat(parts[18].trim()));
                        rt.setOrderdate(parts[19].trim());
                        rt.setOrderpriority(parts[20].trim());
                        rt.setClerk(parts[21].trim());
                        rt.setShippriority(Integer.parseInt(parts[22].trim()));
                        rt.setOrdercomment(parts[23].trim());
                        rt.setMessagedate(parts[24].trim());
                        return rt;
                    }
                }
        );
        DataFrame df = sqlContext.createDataFrame(fRDD, RealTime.class);
        df.registerTempTable("realtime");
        df.printSchema();
        DataFrame result = sqlContext.sql(sql);
        result.show();
        System.out.println("Count: " + df.count() + ", Result: " + result.count());
    }

    public static Set<String> queryMeta(String metaSQL) {
        Set<String> fileList = new HashSet<String>();
//        StringBuilder sb = new StringBuilder();
//        sb.append("SELECT file FROM metatable WHERE partition IN (");
//        for (int i = 0; i < partition.size()-1; i++) {
//            sb.append(partition.get(i) + ", ");
//        }
//        sb.append(partition.get(partition.size()-1) + ")");
//        if (!beginTime.equalsIgnoreCase("BEGIN")) {
//            String[] parts = beginTime.split("-");
//            beginTime = parts[0]+"-"+parts[1]+"-"+parts[2]+" "+parts[3];
//            sb.append(" AND beginTime >= '" + beginTime + "'");
//        }
//        if (!endTime.equalsIgnoreCase("END")) {
//            String[] parts = endTime.split("-");
//            endTime = parts[0]+"-"+parts[1]+"-"+parts[2]+" "+parts[3];
//            sb.append(" AND endTime <= '" + endTime + "'");
//        }
//        sb.append(";");
//        System.out.println(sb.toString());
        ResultSet resultSet = dbConnection.execQuery(metaSQL);
        try {
            while (resultSet.next()) {
                fileList.add(resultSet.getString("file"));
            }
            resultSet.close();
            return fileList;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static class RealTime {
        private int custkey;
        private int orderkey;
        private int partkey;
        private int suppkey;
        private int linenumber;
        private float quantity;
        private float extendedprice;
        private float discount;
        private float tax;
        private String returnflag;
        private String linestatus;
        private String shipdate;
        private String commitdate;
        private String receiptdate;
        private String shipinstruct;
        private String shipmode;
        private String comment;
        private String orderstatus;
        private float totalprice;
        private String orderdate;
        private String orderpriority;
        private String clerk;
        private int shippriority;
        private String ordercomment;
        private String messagedate;

        public int getCustkey() {
            return custkey;
        }

        public void setCustkey(int custkey) {
            this.custkey = custkey;
        }

        public void setPartkey(int partkey) {
            this.partkey = partkey;
        }

        public int getPartkey() {
            return partkey;
        }

        public int getOrderkey() {
            return orderkey;
        }

        public void setOrderkey(int orderkey) {
            this.orderkey = orderkey;
        }

        public int getSuppkey() {
            return suppkey;
        }

        public void setSuppkey(int suppkey) {
            this.suppkey = suppkey;
        }

        public int getLinenumber() {
            return linenumber;
        }

        public void setLinenumber(int linenumber) {
            this.linenumber = linenumber;
        }

        public float getQuantity() {
            return quantity;
        }

        public void setQuantity(float quantity) {
            this.quantity = quantity;
        }

        public float getExtendedprice() {
            return extendedprice;
        }

        public void setExtendedprice(float extendedprice) {
            this.extendedprice = extendedprice;
        }

        public float getDiscount() {
            return discount;
        }

        public void setDiscount(float discount) {
            this.discount = discount;
        }

        public float getTax() {
            return tax;
        }

        public void setTax(float tax) {
            this.tax = tax;
        }

        public String getReturnflag() {
            return returnflag;
        }

        public void setReturnflag(String returnflag) {
            this.returnflag = returnflag;
        }

        public String getLinestatus() {
            return linestatus;
        }

        public void setLinestatus(String linestatus) {
            this.linestatus = linestatus;
        }

        public String getShipdate() {
            return shipdate;
        }

        public void setShipdate(String shipdate) {
            this.shipdate = shipdate;
        }

        public String getCommitdate() {
            return commitdate;
        }

        public void setCommitdate(String commitdate) {
            this.commitdate = commitdate;
        }

        public String getReceiptdate() {
            return receiptdate;
        }

        public void setReceiptdate(String receiptdate) {
            this.receiptdate = receiptdate;
        }

        public String getShipinstruct() {
            return shipinstruct;
        }

        public void setShipinstruct(String shipinstruct) {
            this.shipinstruct = shipinstruct;
        }

        public String getShipmode() {
            return shipmode;
        }

        public void setShipmode(String shipmode) {
            this.shipmode = shipmode;
        }

        public String getComment() {
            return comment;
        }

        public void setComment(String comment) {
            this.comment = comment;
        }

        public String getOrderstatus() {
            return orderstatus;
        }

        public void setOrderstatus(String orderstatus) {
            this.orderstatus = orderstatus;
        }

        public float getTotalprice() {
            return totalprice;
        }

        public void setTotalprice(float totalprice) {
            this.totalprice = totalprice;
        }

        public String getOrderdate() {
            return orderdate;
        }

        public void setOrderdate(String orderdate) {
            this.orderdate = orderdate;
        }

        public String getOrderpriority() {
            return orderpriority;
        }

        public void setOrderpriority(String orderpriority) {
            this.orderpriority = orderpriority;
        }

        public String getClerk() {
            return clerk;
        }

        public void setClerk(String clerk) {
            this.clerk = clerk;
        }

        public int getShippriority() {
            return shippriority;
        }

        public void setShippriority(int shippriority) {
            this.shippriority = shippriority;
        }

        public String getOrdercomment() {
            return ordercomment;
        }

        public void setOrdercomment(String ordercomment) {
            this.ordercomment = ordercomment;
        }

        public String getMessagedate() {
            return messagedate;
        }

        public void setMessagedate(String messagedate) {
            this.messagedate = messagedate;
        }
    }
}
