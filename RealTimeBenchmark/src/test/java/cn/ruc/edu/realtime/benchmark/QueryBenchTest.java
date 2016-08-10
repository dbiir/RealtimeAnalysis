//package cn.ruc.edu.realtime.benchmark;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.execution.datasources.DataSource;
//import scala.collection.JavaConversions;
//import scala.collection.Seq;
//
//import java.io.Serializable;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.util.*;
//
///**
// * RealTimeAnalysis
// *
// * @author Jelly
// */
//public class QueryBenchTest {
////    private static DBConnection dbConnection = new PostgresConnection();
//    private static String beginTime;
//    private static String endTime;
//    private static SparkSession spark;
//
//    public QueryBenchTest() {
//        spark = SparkSession
//                .builder()
//                .appName("QueryBench")
//                .master("spark://192.168.7.33:7077")
//                .getOrCreate();
//    }
//
//    public static void main(String[] args) {
//        QueryBenchTest test = new QueryBenchTest();
////        Dataset<Row> df = spark.read().load(args[0]);
////        System.out.println("Schema: " + df.schema());
////        System.out.println("Count: " + df.count());
//
//        List<Square> squares = new ArrayList();
//        for (int value = 1; value <= 5; value++) {
//            Square square = new Square();
//            square.setValue(value);
//            square.setSquare(value * value);
//            squares.add(square);
//        }
//
//        // Create a simple DataFrame, store into a partition directory
//        Dataset<Row> squaresDF = spark.createDataFrame(squares, Square.class);
//        squaresDF.write().parquet(args[0]);
//
//        Dataset<Row> df = spark.read().format("org.apache.spark.sql.execution.datasources.parquet").load(args[0]);
//        System.out.println("Count: " + df.count());
//
////        List<Integer> partitions = new ArrayList<Integer>();
////        if (args.length < 3) {
////            System.out.println("beginTime endTime fiberId fiberId ...");
////            System.exit(-1);
////        }
////        beginTime = args[0];
////        endTime = args[1];
////        QueryBenchTest bench = new QueryBenchTest();
////
////        System.out.println(beginTime + " -:- " + endTime);
////        for (int i = 2; i < args.length; i++) {
////            partitions.add(Integer.parseInt(args[i]));
////        }
////        long before = System.currentTimeMillis();
////        Set<String> files = bench.queryMeta(partitions, beginTime, endTime);
//
////        for (String file: files) {
////            System.out.println(file);
////        }
////        System.out.println("Array Total: " + files.size());
//
////        StringBuilder sb = new StringBuilder();
////        sb.append("select sum(quantity) as sum_qty, sum(extendedprice) as sum_base_price, " +
////                "avg(quantity) as avg_qty, avg(extendedprice) as avg_price, avg(discount) as avg_disc, " +
////                "count(*) as count_order, min(orderkey) as min_orderkey, max(orderkey) as max_orderkey from realtime");
////        if (beginTime != "BEGIN") {
////            sb.append(" WHERE beginTime >= '" + beginTime + "'");
////        }
////        if (endTime != "END") {
////            sb.append(" AND endTime <= '" + endTime + "'");
////        }
////        String[] fileA = files.toArray(new String[files.size()]);
////        Seq<String> seq = JavaConversions.asScalaSet(files).toSeq();
////        Iterator iterator = (Iterator) seq.toIterator();
////        while (iterator.hasNext()) {
////            System.out.println("PATH:" + iterator.next());
////        }
//
////        bench.querySparkSQL(fileA, sb.toString());
////        bench.querySparkSQL(new String[1], sb.toString());
////        long end = System.currentTimeMillis();
////        System.out.println("Query cost: " + (end - before) + " ms");
//    }
//
//    /**
//     * Generate Spark SQL queries.
//     * @return original query sentence
//     * */
////    public String queryGenerator() {
////        // SELECT * FROM TABLE;
////        // SELECT * FROM TABLE WHERE T1 < t < T2 AND custkey=
////    }
//
//    public void querySparkSQL(String[] files, String query) {
////        Dataset<Row> dataset = spark.read().parquet("hdfs://192.168.7.33:9000/jelly/parquet/48494041424344454647147025962319614702605327966.278860442636987E11");
//        Dataset<Row> dataset = spark.read().format("org.apache.spark.sql.execution.datasources.text.DefaultSource").text("hdfs://192.168.7.33:9000/text/0146981744583814698174654782.0194399064028755E12");
//        dataset.createOrReplaceTempView("realtime");
//        Dataset<Row> result = spark.sql(query);
//        result.show();
//    }
//
//    /**
//     * Query MetaDB, get query files
//     * */
//    public Set<String> queryMeta(List<Integer> partition, String beginTime, String endTime) {
//        Set<String> fileList = new HashSet<String>();
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
////        ResultSet resultSet = dbConnection.execQuery(sb.toString());
////        try {
////            while (resultSet.next()) {
////                fileList.add(resultSet.getString("file"));
////            }
////            resultSet.close();
////            return fileList;
////        } catch (SQLException e) {
////            e.printStackTrace();
////        }
//        return null;
//    }
//
//    public static class Square implements Serializable {
//        private int value;
//        private int square;
//
//        // Getters and setters...
//        // $example off:schema_merging$
//        public int getValue() {
//            return value;
//        }
//
//        public void setValue(int value) {
//            this.value = value;
//        }
//
//        public int getSquare() {
//            return square;
//        }
//
//        public void setSquare(int square) {
//            this.square = square;
//        }
//        // $example on:schema_merging$
//    }
//}
