package Chapter6

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
object WorkingWithNull extends App {

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
    * Using Coalesce == allow you to select the first non-null value from a set of columns
    */
  import org.apache.spark.sql.functions.coalesce
  df.select(coalesce(col("Description"), col("CustomerId"))).show()

  /**
    * =====================ifnull, nullIf, nvl, and nvl2 =====================
    * ifnull : allows you to select the second value if the first is null, and defaults to the first
    * Nullif : which returns null if the two values are equal or else returns the second if they are not
    * Nvl : second value if the first is null, but defaults to the first.
    * nvl2: returns the second value if the first is not null; otherwise, it will return the last specified value
    *
    * //use case
    *
    * -- in SQL
    * SELECT
    * ifnull(null, 'return_value'),
    * nullif('value', 'value'),
    * nvl(null, 'return_value'),
    * nvl2('not_null', 'return_value', "else_value") //returns else_value in this
    * FROM dfTable LIMIT 1
    *
    *
    */


  /**
    * =====================drop=====================
    * The simplest function is drop, which removes rows that contain nulls.
    * //The default is to drop any row in which any value is null:
    *
    */

  df.na.drop()
  df.na.drop("any")	//drops a row if any of the values are null.
  df.na.drop("all")	//drops the row only if all values are null or NaN for that row:
  df.na.drop("all", Seq("StockCode", "InvoiceNo")) // applies to certain sets of columns

  /**
    * =====================fill=====================
    * You can fill one or more columns with a set of values.
    * This can be done by specifying a map—that is a particular value and a set of columns.
    */



  df.na.fill("All Null values become this string")
  df.na.fill(5:Long) // columns of type Long
//  df.na.fill(5:Integer) //No support for integer
  df.na.fill(5:Double) //  for Doubles
  df.na.fill(5, Seq("StockCode", "InvoiceNo")) //with specific columns
  //Using scala map [diff values for diff columns]
  val fillColValues = Map("StockCode" -> 5, "Description" -> "No Value")
  df.na.fill(fillColValues)


  /**
    * =====================replace=====================
    * Replace all values in a certain column according to their current value. The only requirement is that this value be the same type as the original value:
    *
    */
  df.na.replace("Description", Map("" -> "UNKNOWN"))
}