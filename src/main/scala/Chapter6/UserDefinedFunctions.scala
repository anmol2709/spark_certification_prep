package Chapter6

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
object UserDefinedFunctions extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("APIFunctions")

  val spark = SparkSession.builder
    .config(conf = conf)
    .appName("spark session example")
    .getOrCreate()


  // in Scala
  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/home/anmols/Desktop/Spark_Certification_preperation/Spark-The-Definitive-Guide/data/retail-data/by-day/2010-12-01.csv")
  df.printSchema()
  df.createOrReplaceTempView("dfTable")


  /**
    * Creating a UDF
     */
  val udfExampleDF = spark.range(5).toDF("num")
  def power3(number:Double):Double = number * number * number
  power3(2.0)

  /**
    *Register them on spark so that we can use them on all of our worker machines.
    * Spark will serialize the function on the driver and transfer it over the network to all executor processes.
    */

  // in Scala
  import org.apache.spark.sql.functions.udf
  val power3udf = udf(power3(_:Double):Double)


  /**
    * Select using UDF
    */

  udfExampleDF.select(power3udf(col("num"))).show()


}
