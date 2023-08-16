package spark3.sql

import com.beust.jcommander.JCommander
import org.apache.spark.sql.SparkSession

import scala.io.Source

object SqlWrapper {
  def main(args: Array[String]): Unit = {

    val opt = new CliOptions
    val cmd = new JCommander(opt, null, args: _*)

    val spark = SparkSession.builder()
      .appName("Spark Hudi Wrapper")
      .config("spark.master", "local[*]")
      .config("spark.default.parallelism", "4")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.driver.maxResultSize", "2g")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
//      .config("spark.sql.hive.convertMetastoreParquet", "false")
      .config("spark.hadoop.mapred.output.compress", "true")
      .config("spark.hadoop.mapred.output.compression.codec", "true")
      .config("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
      .config("spark.hadoop.mapred.output.compression.type", "BLOCK")
//      .config("hoodie.datasource.hive_sync.jdbcurl", "jdbc:hive2://localhost:10000")
//      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    //    import spark.implicits._
    spark.sparkContext.hadoopConfiguration.addResource("core-site.xml")
    spark.sparkContext.hadoopConfiguration.addResource("hdfs-site.xml")

    var df = spark.read.option("delimiter", "|")
      .csv("/data/2/call_center")

    println(s"Read df ${df.count()}")

/*
    val sqlFileName = s"q0.sql"

    val path: String = "queries/" + sqlFileName
    val tpsSql = Source.fromResource(path).mkString
    spark.sql(s"use ${opt.database}")
    println(s"Database: ${opt.database}, SQL file: $sqlFileName")
    val start = System.nanoTime()
    val df = spark.sql(tpsSql.stripMargin)
    val elapsed = (System.nanoTime() - start) / 1000000000D
    df.show()
    println(s"Elapsed time: $elapsed seconds")
*/
    spark.stop()
  }
}
