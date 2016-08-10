//import org.apache.spark.sql.SparkSession
//
//object QueryBench {
//
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession
//      .builder()
//      .appName("QueryBench")
//      .master("spark://192.168.7.33:7077")
//      .getOrCreate();
//
//    runBasicDataFrameExample(spark)
//    spark.stop()
//  }
//
//  private def runBasicDataFrameExample(spark: SparkSession): Unit = {
//    val df = spark.read.text("hdfs://192.168.7.33:9000/text/0146981744583814698174654782.0194399064028755E12")
//    df.show(10)
//  }
//}