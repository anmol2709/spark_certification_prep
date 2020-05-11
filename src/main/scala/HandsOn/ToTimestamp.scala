package HandsOn

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ToTimestamp extends App{
  val conf = new SparkConf().setMaster("local[*]").setAppName("APIFunctions")

  val spark = SparkSession.builder
    .config(conf = conf)
    .appName("spark session example")
    .getOrCreate()
  import spark.implicits._

  import org.apache.spark.sql.functions._
  var df = spark.sparkContext.parallelize(Seq("2018-07-01T00:00:00Z")).toDF("date_string")

  df = df.withColumn("timestamp", $"date_string".cast("timestamp"))
  df = df.withColumn("epoch_seconds", $"timestamp".cast("long"))

/*  val newDF1= df.select(col("epoch_seconds")).withColumn("newcol",col("epoch_seconds").cast("timestamp"))
    .withColumn("month",month($"newcol")).withColumn("day",dayofmonth($"newcol"))
  newDF1.show()
//  df.show(false)*/


  val timestampCastedDF= df.select(col("epoch_seconds").cast("timestamp").as("timestamp"))
timestampCastedDF.show()


  val organisedDF=timestampCastedDF.select(col("timestamp")
    ,month(col("timestamp")).as("month")
    ,dayofmonth(col("timestamp")).as("dayofmonth")
    ,dayofyear(col("timestamp")).as("dayofyear")
  )
  organisedDF.show()
  organisedDF.filter(col("month")===7).show()
  organisedDF.filter(col("month")===8).show()
}

