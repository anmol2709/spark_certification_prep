package Chapter10_SQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SQLQueries  extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("APIFunctions")

  val spark = SparkSession.builder
    .config(conf = conf)
    .appName("spark session example")
    .getOrCreate()
  import spark.implicits._

  /**
    * GENERIC READ API
    * DataframeReader[spark.read].format(...).option("key", "value").schema(...).load()
    */

  //Example:
  spark.read.format("csv")
    .option("mode", "FAILFAST")
    .option("inferSchema", "true")
    .option("path", "path/to/file(s)") //change this
    .schema("SomeSchema") //this also
    .load()


  /**
    * Interoperability between Sql and Dataframe
    */

  // DF => SQL

  spark.read.json("/home/anmols/Desktop/Spark_Certification_preperation/Spark-The-Definitive-Guide/data/flight-data/json/2015-summary.json")
    .createOrReplaceTempView("some_sql_view")


  // SQL => DF
  spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count)FROM some_sql_view GROUP BY DEST_COUNTRY_NAME
""")
    .where("DEST_COUNTRY_NAME like 'S%'").where("`sum(count)` > 10")
    .count()

}
