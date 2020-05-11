package Chapter7_Aggregations

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Windowing extends App {

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
    * First step to a window function is to create a window specification
    *
    * PartitionBy - describes how we will be breaking up our group
    * OrderBy - ordering within a given partition,
    * Frame specification - the rowsBetween statement) states
    *                       which rows will be included in the frame based on its reference to the current input row
    */
  import org.apache.spark.sql.expressions.Window
  import org.apache.spark.sql.functions.col
  val windowSpec = Window
    .partitionBy("CustomerId", "date")
    .orderBy(col("Quantity").desc)
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)


  /**
    * Using a aggregation function
    * Example : establishing the maximum purchase quantity over all time
    * Returns column (or expressions)
    */
    import org.apache.spark.sql.functions.max
  val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)

  /**
    * we will create the purchase quantity rank.
    * To do that we use the dense_rank function  ==> to determine which date had the maximum purchase quantity for every customer
    * Returns column (or expressions)
    *
    *
    * RANK VS DENSE RANK
    * We use dense_rank as opposed to rank to avoid gaps in the ranking sequence when there
    * are tied values
    */
  // in Scala
  import org.apache.spark.sql.functions.{dense_rank, rank}
  val purchaseDenseRank = dense_rank().over(windowSpec)
  val purchaseRank = rank().over(windowSpec)

    /**
      * Select to see result
      */

  dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")
    .select(
      col("CustomerId"),
      col("date"),
      col("Quantity"),
      purchaseRank.alias("quantityRank"),
      purchaseDenseRank.alias("quantityDenseRank"),
      maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()



}
