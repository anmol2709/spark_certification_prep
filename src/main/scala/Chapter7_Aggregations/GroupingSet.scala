package Chapter7_Aggregations

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object GroupingSet extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("APIFunctions")

  val spark = SparkSession.builder
    .config(conf = conf)
    .appName("spark session example")
    .getOrCreate()

  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/home/anmols/Desktop/Spark_Certification_preperation/Spark-The-Definitive-Guide/data/retail-data/all/*.csv")
    .coalesce(5)      //moving the partitition to 5
  df.cache()        //caching
  df.createOrReplaceTempView("dfTable")

  /**
    * we will add a date column that will convert our invoice date into a column that
    * contains only date information (not time information, too):
    */
  import org.apache.spark.sql.functions.{col, to_date}
  val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"),
    "MM/d/yyyy H:mm"))
  dfWithDate.createOrReplaceTempView("dfWithDate")
  /**
    * Removing nulls
    * Grouping sets depend on null values for aggregation levels.
    * If you do not filter-out null values, you will get incorrect results.
    */
  val dfNoNull = dfWithDate.drop()
  dfNoNull.createOrReplaceTempView("dfNoNull")


  /**
    * ROLL UP
    * A rollup is a multidimensional aggregation that performs a variety of group-by style calculations for us
    * USE CASE :Let’s create a rollup that looks across time (with our new Date column)
    * and space (with the Country column) and creates a new DataFrame that includes the grand total over all dates,
    * the grand total for each date in the DataFrame, and the subtotal for each country on each date in the DataFrame:
    */

  import org.apache.spark.sql.functions.sum

  val rolledUpDF = dfNoNull.rollup("Date", "Country").agg(sum("Quantity"))
    .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")
    .orderBy("Date")

  rolledUpDF.show()

  //A null in both rollup columns specifies the grand total across both of those columns:

    rolledUpDF.where("Country IS NULL").show()
  rolledUpDF.where("Date IS NULL").show()

  /**
    * CUBE
    * A cube takes the rollup to a level deeper
    * Rather than treating elements hierarchically, a cube does the same thing across all dimensions
    * This means that it won’t just go by date over the entire time period, but also the country
    */


  dfNoNull.cube("Date", "Country").agg(sum(col("Quantity")))
    .select("Date", "Country", "sum(Quantity)").orderBy("Date").show()


  /**
    * GROUPING Metadata ==> query the aggregation levels
    *
  //0 => no value null
  //1=> customerId null
  //2 => stockCode null
  //3 => both null

    */
  import org.apache.spark.sql.functions.{grouping_id, sum, expr}
  dfNoNull.cube("customerId", "stockCode").agg(grouping_id(), sum("Quantity"))
    .orderBy(expr("grouping_id()").desc)
    .show()


  /**
    * Pivot == > Pivots make it possible for you to convert a row into a column.
  //DataFrame will now have a column for every combination of country, numeric variable, and a column specifying the date.
    */
  val pivoted = dfWithDate.groupBy("date").pivot("Country").sum()

  pivoted.where("date > '2011-12-05'").select("date" ,"`USA_sum(Quantity)`").show()
//gives only one column


}
