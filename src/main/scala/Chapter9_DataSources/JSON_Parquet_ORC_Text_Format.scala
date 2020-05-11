package Chapter9_DataSources

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class JSON_Parquet_ORC_Text_Format extends App {

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


  val csvFile = spark.read.format("csv")
    .option("header", "true").option("mode", "FAILFAST").schema(myManualSchema)
    .load("/home/anmols/Desktop/Spark_Certification_preperation/Spark-The-Definitive-Guide/data/flight-data/csv/2010-summary.csv")

  /**
    * Read Json
    */

  spark.read.format("json").option("mode", "FAILFAST").schema(myManualSchema)
    .load("/home/anmols/Desktop/Spark_Certification_preperation/Spark-The-Definitive-Guide/data/flight-data/json/2010-summary.json").show(5)


  /**
    * Writing json
    */
  //the data source does not matter.

  csvFile.write.format("json").mode("overwrite").save("/tmp/my-json-file.json")

  /**
    * ========================PARQUET FILE========================
    */

  /**
    * Reading PARQUET
    */

  spark.read.format("parquet")
    .load("/home/anmols/Desktop/Spark_Certification_preperation/Spark-The-Definitive-Guide/data/flight-data/parquet/2010-summary.parquet").show(5)

  /**
    * Writing to parquet
    */
  csvFile.write.format("parquet").mode("overwrite")
    .save("/tmp/my-parquet-file.parquet")

  /**
    * ========================ORC FILE========================
    */

  /**
    * Reading:
    */
    spark.read.format("orc").load("/home/anmols/Desktop/Spark_Certification_preperation/Spark-The-Definitive-Guide/data/flight-data/orc/2010-summary.orc").show(5)
  /**
    * Writing:
    */
  csvFile.write.format("orc").mode("overwrite").save("/tmp/my-json-file.orc")

  /**
    * ========================TEXT FILE========================
    */

  /**
    * Reading:
    */
  spark.read.textFile("/data/flight-data/csv/2010-summary.csv")
    .selectExpr("split(value, ',') as rows").show()


  /**
    * Writing:
    */
  csvFile.select("DEST_COUNTRY_NAME").write.text("/tmp/simple-text-file.txt")

  /**
    * If you perform some partitioning when performing your write , you can write more columns.
    */
  csvFile.limit(10).select("DEST_COUNTRY_NAME", "count")
    .write.partitionBy("count").text("/tmp/five-csv-files2.csv")
}
