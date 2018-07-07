package com.insight.app.DataTrends

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions._

/*
 * Compute moving average for bid offers usnig miving window of 10 seconds and waiting for late data for 2 minutes.
 */

object MovingAvg {
  def main(args: Array[String]) {

    val broker = "ec2-18-209-75-68.compute-1.amazonaws.com:9092,ec2-18-205-142-57.compute-1.amazonaws.com:9092,ec2-50-17-32-144.compute-1.amazonaws.com:9092"

    val spark = SparkSession
      .builder
      .appName("MovingAverage")
      .getOrCreate()

    import spark.implicits._

    // Define input stream from Kafka
    val dfraw = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", broker)
      .option("subscribe", "currency_exchange")
      .load()

    // Define schema for incommint data
    val schema = StructType(
      Seq(
        StructField("fx_marker", StringType, false),
        StructField("timestamp_ms", StringType, false),
        StructField("bid_big", StringType, false),
        StructField("bid_points", StringType, false),
        StructField("offer_big", StringType, false),
        StructField("offer_points", StringType, false),
        StructField("hight", StringType, false),
        StructField("low", StringType, false),
        StructField("open", StringType, false)
      )
    )

  // Parse incomming stream
  val df = dfraw
   .selectExpr("CAST(value AS STRING)").as[String]
    .flatMap(_.split("\n"))

  val jsons = df.select(from_json($"value", schema) as "data").select("data.*")

  val parsed = jsons
    .withColumn("bid_big", $"bid_big".cast(DoubleType))
    .withColumn("bid_points", $"bid_points".cast(IntegerType))
    .withColumn("offer_big", $"offer_big".cast(DoubleType))
    .withColumn("offer_points", $"offer_points".cast(IntegerType))
    .withColumn("hight", $"hight".cast(DoubleType))
    .withColumn("low", $"low".cast(DoubleType))
    .withColumn("open", $"open".cast(DoubleType))
    .withColumn("timestamp_dt", to_timestamp(from_unixtime($"timestamp_ms"/1000.0, "yyyy-MM-dd HH:mm:ss.SSS")))
    .drop("_tmp").filter("fx_marker != ''")

  // Compute moving average
  val projected = parsed.select($"fx_marker", $"timestamp_ms", $"timestamp_dt", $"bid_big", $"bid_points")

  val windAvg = projected
    .withWatermark("timestamp_dt", "2 minutes")
    .groupBy(
      window($"timestamp_dt", "30 seconds", "5 seconds"),
      $"fx_marker"
    ).mean()

  // Define data sink
  val sinkKafkaAvg = windAvg
    .selectExpr("CAST(fx_marker AS STRING) AS key", "to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", broker)
    .option("topic", "fx_avg")
    .option("checkpointLocation", sys.env("HOME") + "/kafka_sink_chkp/sink_windAvg")
    .outputMode("update")
    .start()

    sinkKafkaAvg.awaitTermination()
  }
}
