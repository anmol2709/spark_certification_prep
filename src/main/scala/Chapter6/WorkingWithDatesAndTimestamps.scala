package Chapter6

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object WorkingWithDatesAndTimestamps extends App {

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
    * get the current date and the current timestamps:
    */

  import org.apache.spark.sql.functions.{current_date, current_timestamp}
  val dateDF = spark.range(10)
    .withColumn("today", current_date())
    .withColumn("now", current_timestamp())
  dateDF.createOrReplaceTempView("dateTable")

dateDF.printSchema()
  //root
  // |-- id: long (nullable = false)
  // |-- today: date (nullable = false)
  // |-- now: timestamp (nullable = false)


  /**
    * add and subtract five days from today.
    */
  import org.apache.spark.sql.functions.col
  import org.apache.spark.sql.functions.lit
  import org.apache.spark.sql.functions.{date_add, date_sub}
  dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)

  /**
    *  datediff / months_between between 2 dates
    */
  import org.apache.spark.sql.functions.{datediff, months_between, to_date}

import org.apache.spark.sql.functions.{datediff, months_between, to_date}
dateDF.withColumn("week_ago", date_sub(col("today"), 7))
.select(datediff(col("week_ago"), col("today"))).show(1)
dateDF.select(
to_date(lit("2016-01-01")).alias("start"),
to_date(lit("2017-05-22")).alias("end"))
.select(months_between(col("start"), col("end"))).show(1)


  /**
    * Convert String to Date    ==> to_date
    */
  // in Scala
  import org.apache.spark.sql.functions.{to_date, lit}
  spark.range(5).withColumn("date", lit("2017-01-01"))
    .select(to_date(col("date"))).show(1)

  /**
    * format should be valid else null
    */
  dateDF.select(to_date(lit("2016-20-12")),to_date(lit("2017-12-11"))).show(1)

  /**
    * to_date and to_timestamp.
    *
    *
    * converts  2017-12-11  to 2017-11-12
    */

  // to_date
  import org.apache.spark.sql.functions.to_date
  val dateFormat = "yyyy-dd-MM"
  val cleanDateDF = spark.range(1).select(
    to_date(lit("2017-12-11"), dateFormat).alias("date"),
    to_date(lit("2017-20-12"), dateFormat).alias("date2"))
  cleanDateDF.createOrReplaceTempView("dateTable2")


//  to_timestamp
  /**
    * Casting between dates and timestamps is simple in all languages—in SQL
    * SELECT cast(to_date("2017-01-01", "yyyy-dd-MM") as timestamp)
    */

  /*
  |to_timestamp(`date`, 'yyyy-dd-MM')|
+----------------------------------+
|               2017-11-12 00:00:00|
+----------------------------------+
   */

  import org.apache.spark.sql.functions.to_timestamp
  cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()

  //both are same
  cleanDateDF.filter(col("date2") > lit("2017-12-12")).show()
    cleanDateDF.filter(col("date2") > "'2017-12-12'").show()
}