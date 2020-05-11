package Chapter6

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object WorkingWithComplexTypes extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("APIFunctions")

  val spark = SparkSession.builder
    .config(conf = conf)
    .appName("spark session example")
    .getOrCreate()
  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/home/anmols/Desktop/Spark_Certification_preperation/Spark-The-Definitive-Guide/data/retail-data/by-day/2010-12-01.csv")
  df.printSchema()
  df.createOrReplaceTempView("dfTable")


  /**
    * a=================================STRUCT===============================
    * We can create a struct by wrapping a set of columns in parenthesis in a query
    */

  df.selectExpr("(StockCode, InvoiceNo) as complex", "*").show(2)

  import org.apache.spark.sql.functions.struct

  val complexDF = df.select(struct("StockCode", "InvoiceNo").alias("complex"))
  complexDF.createOrReplaceTempView("complexDF")

  //to query
  complexDF.select("complex.Description")
  complexDF.select(col("complex").getField("Description"))
  complexDF.select("complex.*") //query all the values in a struct



  /**
    * a=================================ARRAYS===============================
    */
  import org.apache.spark.sql.functions.split
  df.select(split(col("Description"), " ")).show(2)

// to check
   df.select(split(col("Description"), " ").alias("array_col")).selectExpr("array_col[0]").show(2)

  /**
    * Array size
    */
  // in Scala
  import org.apache.spark.sql.functions.size
  df.select(size(split(col("Description"), " "))).show(2) // shows 5 and 3

  /**
    * array_contains
    * To see whether this array contains a value:
    */
   import org.apache.spark.sql.functions.array_contains
   df.select(array_contains(split(col("Description"), " "), "WHITE")).show(2)

  /**
    * Explode
    * The explode function takes a column that consists of arrays and creates one row (with the rest of the values duplicated) per value in the array.
    */


  import org.apache.spark.sql.functions.{split, explode}
  df.withColumn("splitted", split(col("Description"), " "))
    .withColumn("exploded", explode(col("splitted")))
    .select("Description", "StockCode", "exploded").show(6)


  /**
    * a=================================MAPS===============================
    */

  import org.apache.spark.sql.functions.map
  df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map")).show(2)

  /**
    * Querying a map
    * You can query them by using the proper key. A missing key returns null
    */

  import org.apache.spark.sql.functions.map
  df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map")).show(2)
}
