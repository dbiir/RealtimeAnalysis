//package cn.ruc.edu.realtime.benchmark;
//
//import cn.edu.ruc.realtime.utils.DBConnection;
//import cn.edu.ruc.realtime.utils.PostgresConnection;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//import scala.collection.JavaConversions;
//import scala.collection.Seq;
//
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Set;
//
///**
// * RealTimeAnalysis
// *
// * @author Jelly
// */
//public class QueryBench {
//    private static DBConnection dbConnection = new PostgresConnection();
//    private static String beginTime;
//    private static String endTime;
//    private static SparkSession spark;
//
//    public QueryBench() {
//        spark = SparkSession
//                .builder()
//                .appName("QueryBench")
//                .master("scujgd01:7077")
//                .getOrCreate();
//    }
//
//    public static void main(String[] args) {
//        QueryBench bench = new QueryBench();
//        List<Integer> partitions = new ArrayList<Integer>();
//        if (args.length < 3) {
//            System.out.println("beginTime endTime fiberId fiberId ...");
//        }
//        beginTime = args[0];
//        endTime = args[1];
//
//        System.out.println(beginTime + " -:- " + endTime);
//        for (int i = 2; i < args.length; i++) {
//            partitions.add(Integer.parseInt(args[i]));
//        }
//        long before = System.currentTimeMillis();
//        Set<String> files = bench.queryMeta(partitions, beginTime, endTime);
//
//        for (String file: files) {
//            System.out.println(file);
//        }
//        System.out.println("Total: " + files.size());
//
//        StringBuilder sb = new StringBuilder();
//        sb.append("select sum(quantity) as sum_qty, sum(extendedprice) as sum_base_price, " +
//                "avg(quantity) as avg_qty, avg(extendedprice) as avg_price, avg(discount) as avg_disc, " +
//                "count(*) as count_order, min(orderkey) as min_orderkey, max(orderkey) as max_orderkey from realtime");
//        if (beginTime != "BEGIN") {
//            sb.append(" WHERE beginTime >= '" + beginTime + "'");
//        }
//        if (endTime != "END") {
//            sb.append(" AND endTime <= '" + endTime + "'");
//        }
//        Seq<String> seq = JavaConversions.asScalaSet(files).toSeq();
//
//        bench.querySparkSQL(seq, sb.toString());
//        long end = System.currentTimeMillis();
//        System.out.println("Query cost: " + (end - before) + " ms");
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
//    public void querySparkSQL(Seq<String> files, String query) {
//        Dataset<Row> dataset = spark.read().parquet(files);
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
//        ResultSet resultSet = dbConnection.execQuery(sb.toString());
//        try {
//            while (resultSet.next()) {
//                fileList.add(resultSet.getString("file"));
//            }
//            resultSet.close();
//            return fileList;
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        return null;
//    }
//}
