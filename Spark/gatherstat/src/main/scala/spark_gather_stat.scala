package com.insight.app.GatherStatistics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._

/*
 * Gather statistics on historical FX rates. Accept range of dates for which to compute statistica as a command line argument.
 */

object SimpleCountStat {
  def main(args: Array[String]) {
    
    if (args.length != 1) {
      println("Usage: '2018-07-05 2018-07-06 2018-07-07'")
      System.exit(1)
    }
    val date_range = args(0).split(" ").toSeq

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "ec2-52-23-103-178.compute-1.amazonaws.com")
    //val sc = new SparkContext("local", "GatherSimpleCountStats", conf)
    val sc = new SparkContext("spark://ec2-18-232-26-53.compute-1.amazonaws.com:7077", "GatherSimpleCountStats", conf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
    val df = sqlContext
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "fx_rates", "keyspace" -> "fx"))
    .load()

    //df.printSchema

    // val date_range = Seq("2018-07-05", "2018-07-06", "2018-07-07")

    val parsed = df.filter(($"fx_marker" === "USD/JPY") && ($"timestamp_d".isin(date_range: _*)))
    .select($"timestamp_ms")

    val statdf = parsed.groupBy(window(parsed.col("timestamp_ms"), "10 seconds")) //$"fx_marker", 
    .count()
    .agg(avg($"count").as("avg_10sec"), stddev($"count").as("sbddev_10sec"))
    .withColumn("fx_marker", lit("USD/JPY"))
    .withColumn("stat_gathered_date", lit(from_utc_timestamp(current_timestamp(), "America/New_York")))

    statdf.write
    .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "anomaly_count_statistics", "keyspace" -> "fx"))
    .mode("append")
    .save()
  }
}
