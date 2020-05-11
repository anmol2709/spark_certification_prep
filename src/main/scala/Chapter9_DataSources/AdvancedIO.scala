package Chapter9_DataSources

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StructField, StructType}

object AdvancedIO extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("APIFunctions")

  val spark = SparkSession.builder
    .config(conf = conf)
    .appName("spark session example")
    .getOrCreate()
  import spark.implicits._

  /**
    *  set the header to true for our CSV file,
    *  the mode to be FAILFAST,
    *  and inferSchema to true
    */

  spark.read.format("csv")
    .option("header", "true")
    .option("mode", "FAILFAST")
    .option("inferSchema", "true")
    .load("/home/anmols/Desktop/Spark_Certification_preperation/Spark-The-Definitive-Guide//data/flight-data/csv/2010-summary.csv")


  val myManualSchema = new StructType(Array(
    new StructField("DEST_COUNTRY_NAME", LongType, true),  //giving long type
    new StructField("ORIGIN_COUNTRY_NAME", LongType, true),
    new StructField("count", LongType, false) ))


  spark.read.format("csv")
    .option("header", "true")
    .option("mode", "FAILFAST")
    .schema(myManualSchema)
    .load("/home/anmols/Desktop/Spark_Certification_preperation/Spark-The-Definitive-Guide/data/flight-data/csv/2010-summary.csv")
    .show(5)



  val csvFile = spark.read.format("csv")
    .option("header", "true").option("mode", "FAILFAST").schema(myManualSchema)
    .load("/home/anmols/Desktop/Spark_Certification_preperation/Spark-The-Definitive-Guide/data/flight-data/csv/2010-summary.csv")


  /**
    * Partitioning
    */
  csvFile.limit(10).write.mode("overwrite").partitionBy("DEST_COUNTRY_NAME")
    .save("/tmp/partitioned-files.parquet")
  /**
    * output:
    * $ ls /tmp/partitioned-files.parquet
    * ...
    * DEST_COUNTRY_NAME=Costa Rica/
    * DEST_COUNTRY_NAME=Egypt/
    * DEST_COUNTRY_NAME=Equatorial Guinea/
    * DEST_COUNTRY_NAME=Senegal/
    * DEST_COUNTRY_NAME=United States/
    */


  /**
    * Bucketing
    * Bucketing is another file organization approach with which you can control the data that is
    * specifically written to each file.
    */

  val numberBuckets = 10
  val columnToBucketBy = "count"
  csvFile.write.format("parquet").mode("overwrite")
    .bucketBy(numberBuckets, columnToBucketBy).saveAsTable("bucketedFiles")



  /**Controlling file size
    * Spark will ensure that
    * files will contain at most 5,000 records.
    */
  val df = spark.read.format("json").load("/home/anmols/Desktop/Spark_Certification_preperation/Spark-The-Definitive-Guide/data/flight-data/json/2015-summary.json")
  df.createOrReplaceTempView("dfTable")


//using this
  df.write.option("maxRecordsPerFile", 5000)


}
