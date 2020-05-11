package Chapter7_Aggregations

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object BasicAggregations extends App {

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

  // count is also grouping

  df.count() == 541909

  /**
    * count - transformation
    */

  import org.apache.spark.sql.functions.count
  df.select(count("StockCode")).show()


  /**
    * countDistinct - number of unique
    */
  import org.apache.spark.sql.functions.countDistinct
  df.select(countDistinct("StockCode")).show() // 4070


  /**
    * approx_count_distinct - when an approximation to a certain degree of accuracy will work just fine
    * You will see much greater performance gains with larger datasets.
    */
  import org.apache.spark.sql.functions.approx_count_distinct
  df.select(approx_count_distinct("StockCode", 0.1)).show() // 3364 // column + maximum estimation error allowed

  /**
    * first and last
    */
  import org.apache.spark.sql.functions.{first, last}
  df.select(first("StockCode"), last("StockCode")).show()

  /**
    * min/max
    */
  import org.apache.spark.sql.functions.{min, max}
  df.select(min("Quantity"), max("Quantity")).show()

  /**
    * sum
    */
  import org.apache.spark.sql.functions.sum
  df.select(sum("Quantity")).show() // 5176450

  /**
    * sumDistinct
    */
  import org.apache.spark.sql.functions.sumDistinct

  df.select(sumDistinct("Quantity")).show() // 29310

  /**
    * avg
    */
  import org.apache.spark.sql.functions.{sum, count, avg, expr}
  df.select(
    count("Quantity").alias("total_transactions"),
    sum("Quantity").alias("total_purchases"),
    avg("Quantity").alias("avg_purchases"),
    expr("mean(Quantity)").alias("mean_purchases"))
    .selectExpr(
      "total_purchases/total_transactions",      // same as avg
      "avg_purchases",
      "mean_purchases").show() // same as avg

  /**
    * Variance and Standard Deviation
    * The variance is the average of the squared differences from the mean, and the standard deviation is the square root of the variance
    */

  import org.apache.spark.sql.functions.{var_pop, stddev_pop}
  import org.apache.spark.sql.functions.{var_samp, stddev_samp}
  df.select(var_pop("Quantity"), var_samp("Quantity"),
    stddev_pop("Quantity"), stddev_samp("Quantity")).show()


  /**
    * skewness and kurtosis
    * Skewness and kurtosis are both measurements of extreme points in your data. Skewness measures the
    * asymmetry of the values in your data around the mean, whereas kurtosis is a measure of the tail of
    * data.
    */

  import org.apache.spark.sql.functions.{skewness, kurtosis}
  df.select(skewness("Quantity"), kurtosis("Quantity")).show()


  /**
    * Covariance and Correlation
    * covar_pop == population covariance
    */
  import org.apache.spark.sql.functions.{corr, covar_pop, covar_samp}
  df.select(corr("InvoiceNo", "Quantity"), covar_samp("InvoiceNo", "Quantity"),
    covar_pop("InvoiceNo", "Quantity")).show()


  /**
    * Aggregating to Complex Types
    *
    * collect_set - returns a set of objects with duplicate elements eliminated.
    * collect_list -  returns a list of objects with duplicates.
    *
    *
    * Use You can use this to carry out some more programmatic access later on in the pipeline or pass the
    * entire collection in a user-defined function (UDF
    */
  import org.apache.spark.sql.functions.{collect_set, collect_list}
  df.agg(collect_set("Country"), collect_list("Country")).show()

}
