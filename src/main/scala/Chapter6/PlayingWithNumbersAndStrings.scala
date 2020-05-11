package Chapter6

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
object PlayingWithNumbersAndStrings extends App {

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
    * POWER
    */
  //Using Power Dataframe api
  import org.apache.spark.sql.functions.{expr, pow}
  val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
  df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(2)

  //Spark Sql

  // in Scala
  df.selectExpr("CustomerId","(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(2)


  /**
    * Round Up=> round
    * Round down if in centre => bround
    */
  import org.apache.spark.sql.functions.{round, bround}
  // in Scala
  import org.apache.spark.sql.functions.lit
  df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)

  /**
    * Pearson correlation coefficient for two columns
    */
  // in Scala
  import org.apache.spark.sql.functions.{corr}
  df.stat.corr("Quantity", "UnitPrice")
  df.select(corr("Quantity", "UnitPrice")).show()

  /**
    * summary statistics    => describe method
    *
    * gives : => count, mean ,stddev , min , max
    *
    * Can also do it manually and apply on column
    * import org.apache.spark.sql.functions.{count, mean, stddev_pop, min, max}
    */

  df.describe().show()

  /**
    * A number of STATISTICAL FUNCTIONS available in the StatFunctions Package
    * Eg:
    * approxQuantile : calculate either exact or approximate quantiles of your data
    *
    * // in Scala
    * val colName = "UnitPrice"
    * val quantileProbs = Array(0.5)
    * val relError = 0.05
    * df.stat.approxQuantile("UnitPrice", quantileProbs, relError) // 2.51
    *
    * cross-tabulation or frequent item pairs
    * // in Scala
    * df.stat.crosstab("StockCode", "Quantity").show()
    * // in Scala
    * df.stat.freqItems(Seq("StockCode", "Quantity")).show()
    *
    * Monotonically_increasing_id : This function generates a unique value for each row,starting with 0
    *
    * import org.apache.spark.sql.functions.monotonically_increasing_id
    * df.select(monotonically_increasing_id()).show(2)
    *
    * random data generation tools (e.g., rand(), randn())  - randomly generate data
    * There are also a number of more advanced tasks like bloom filtering and sketching algorithms available in the stat package
    */


  /**
    * a==============================================STRING OPERATIONS============================================a
    */

  /**
    * initcap : capitalize every word in a given string when that word is separated from another by a space.
    */
  import org.apache.spark.sql.functions.{initcap}
  df.select(initcap(col("Description"))).show(2, false)

  /**
    * cast strings in uppercase and lowercase
    */

  import org.apache.spark.sql.functions.{lower, upper}
  df.select(col("Description"),
    lower(col("Description")),
    upper(lower(col("Description")))).show(2)

  /**
    * Adding or removing spaces around a string
    **/
  import org.apache.spark.sql.functions.{lit, ltrim, rtrim, rpad, lpad, trim}
  df.select(
    ltrim(lit("	  HELLO 	")).as("ltrim"),
    rtrim(lit("	HELLO	")).as("rtrim"),
    trim(lit("	HELLO	")).as("trim"),
    lpad(lit("HELLO"), 3, " ").as("lp"),
    rpad(lit("HELLO"), 10, " ").as("rp")).show(2)

  /**
    *        regexp_replace
    **/


  import org.apache.spark.sql.functions.regexp_replace
  val simpleColors = Seq("black", "white", "red", "green", "blue")
  val regexString = simpleColors.map(_.toUpperCase).mkString("|")
  // the | signifies `OR` in regular expression syntax
  df.select(
    regexp_replace(col("Description"), regexString, "COLOR").alias("color_clean"),
    col("Description")).show(2)


  /**
    *  regexp_extract
    */
  // in Scala
  import org.apache.spark.sql.functions.regexp_extract
  val regexString2 = simpleColors.map(_.toUpperCase).mkString("(", "|", ")")
  // the | signifies OR in regular expression syntax
  df.select(
    regexp_extract(col("Description"), regexString2, 1).alias("color_clean"),
    col("Description")).show(2)


  /**
    * check Existance of value  ===== contains
    * will give rows which contain black or white
    */

  // in Scala
  val containsBlack = col("Description").contains("BLACK")
  val containsWhite = col("DESCRIPTION").contains("WHITE")
  df.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
    .where("hasSimpleColor")
    .select("Description").show(3, false)


  /**
    * >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>  Dynamic number of arguments <<<<<<<<<<<
    */

  // in Scala
  val simpleColors2 = Seq("black", "white", "red", "green", "blue")
  val selectedColumns = simpleColors2.map(color => {
    col("Description").contains(color.toUpperCase).alias(s"is_$color")
  }):+expr("*") // could also append this value
  df.select(selectedColumns:_*).where(col("is_white").or(col("is_red")))
    .select("Description").show(3, false)

  /**
    * REPLACE CHARACTERS
    */
  import org.apache.spark.sql.functions.translate
  df.select(translate(col("Description"), "LEET", "1337"), col("Description"))
    .show(2)

  //L replaced by 1
  //E replaced by 3
  //E replaced by 3
  //T replaced by 7



}




