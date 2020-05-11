package Chapter6

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object APIFunctions extends App {

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
    * LITERAL
    */

  import org.apache.spark.sql.functions.lit
  df.select(lit(5), lit("five"), lit(5.0))
//-- in SQL
  //SELECT 5, "five", 5.0

  /**
    * BOOLEAN
    * In Spark, if you want to filter by equality you should use === (equal) or =!= (not equal). [not == or ===]
    * You can also use the not function and the equalTo method.
    */
 // ⇒ EqualTo Notation
  import org.apache.spark.sql.functions.col
  df.where(col("InvoiceNo").equalTo(536365))
    .select("InvoiceNo", "Description")
    .show(5, false)

 // ⇒ === Notation
  df.where(col("InvoiceNo") === 536365)
    .select("InvoiceNo", "Description")
    .show(5, false)

 // ⇒ expression in a string.
  df.where("InvoiceNo = 536365")
    .show(5, false)
  df.where("InvoiceNo <> 536365")
    .show(5, false)


  // Explicit us of and and or
  // in Scala
  val priceFilter1 = col("UnitPrice") > 600
  val descripFilter1 = col("Description").contains("POSTAGE")
  df.where(col("StockCode").isin("DOT")).where(priceFilter1.or(descripFilter1))
    .show()


  /**
    * Using Boolean as a column
    */

  // in Scala
  val DOTCodeFilter = col("StockCode") === "DOT"
  val priceFilter = col("UnitPrice") > 600
  val descripFilter = col("Description").contains("POSTAGE")
  df.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descripFilter)))
    .where("isExpensive")
    .select("unitPrice", "isExpensive").show(5)


  // in Scala
  import org.apache.spark.sql.functions.{expr, not, col}
  df.withColumn("isExpensive", not(col("UnitPrice").leq(250)))
    .filter("isExpensive")
    .select("Description", "UnitPrice").show(5)

  //USING SPARK SQL expr
  df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))
    .filter("isExpensive")
    .select("Description", "UnitPrice").show(5)

//perrform a null-safe equivalence test
  df.where(col("Description").eqNullSafe("hello")).show()
}

