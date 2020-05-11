package Chapter9_DataSources

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CSVFormat extends App {

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

  /**
    * Providing manual Schema
    */
  //more conformed data
  import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
  val myManualSchema = new StructType(Array(
    new StructField("DEST_COUNTRY_NAME", StringType, true),
    new StructField("ORIGIN_COUNTRY_NAME", StringType, true),
    new StructField("count", LongType, false)
  ))


  spark.read.format("csv")
    .option("header", "true")
    .option("mode", "FAILFAST")
    .schema(myManualSchema)
    .load("/home/anmols/Desktop/Spark_Certification_preperation/Spark-The-Definitive-Guide/data/flight-data/csv/2010-summary.csv")
    .show(5)


  /**
    * Giving wrong schema
    * no compile time error
    * only breaks at runtime
    */


  val myManualSchema1 = new StructType(Array(
    new StructField("DEST_COUNTRY_NAME", LongType, true),  //giving long type
    new StructField("ORIGIN_COUNTRY_NAME", LongType, true),
    new StructField("count", LongType, false) ))


  spark.read.format("csv")
    .option("header", "true")
    .option("mode", "FAILFAST")
    .schema(myManualSchema1)
    .load("/home/anmols/Desktop/Spark_Certification_preperation/Spark-The-Definitive-Guide/data/flight-data/csv/2010-summary.csv")
    .show(5)


  /**
    * Writing CSV Files
    */


  val csvFile = spark.read.format("csv")
    .option("header", "true").option("mode", "FAILFAST").schema(myManualSchema)
    .load("/home/anmols/Desktop/Spark_Certification_preperation/Spark-The-Definitive-Guide/data/flight-data/csv/2010-summary.csv")

  /**
    * Saving tmp files
    * my-tsv-file is actually a folder with numerous files within it:
    *
    * $ ls /tmp/my-tsv-file.tsv/
    * /tmp/my-tsv-file.tsv/part-00000-35cf9453-1943-4a8c-9c82-9f6ea9742b29.csv
    *
    */

  csvFile.write.format("csv").mode("overwrite").option("sep", "\t").save("/tmp/my-tsv-file.tsv")
}

