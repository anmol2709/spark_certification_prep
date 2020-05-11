package HandsOn

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SplitDF_taken_limitn extends App{
  val conf = new SparkConf().setMaster("local[*]").setAppName("APIFunctions")

  val spark = SparkSession.builder
    .config(conf = conf)
    .appName("spark session example")
    .getOrCreate()
  import spark.implicits._

 val df = spark.read.format("csv")
    .option("header", "true")
    .option("mode", "FAILFAST")
    .option("inferSchema", "true")
    .load("/home/anmols/Desktop/Spark_Certification_preperation/Spark-The-Definitive-Guide//data/flight-data/csv/2010-summary.csv")

  val a= df.randomSplit(Array(0.8,0.2))
  print(a(0).count())
  print(a(1).count())

  /**
    * retrieve first 5 rows of 80 percnt as a DF / Dataset
     */

  print(a(0).limit(5).toDF().count())

  /**
    * getting first 5 rows as array
    */
  a(0).take(5).map(print(_))

}
