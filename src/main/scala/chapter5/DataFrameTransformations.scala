package chapter5

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.{expr, col, column}
object DataFrameTransformations extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("Spark-Assignment-2")

  val spark = SparkSession.builder
    .config(conf = conf)
    .appName("spark session example")
    .getOrCreate()



  //Using Literal
val df = spark.read.format("json").load("/home/anmols/Desktop/Spark_Certification_preperation/Spark-The-Definitive-Guide/data/flight-data/json/2015-summary.json")

  df.select(expr("*"), lit(1).as("One")).show(2)


  //Adding column
  df.withColumn("numberOne", lit(1)).show(2)

  //set a Boolean flag for when the origin country is the same as the destination country:
  df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))
    .show(2)

  //Rename column
  df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns

 // Reserved Characters and Keywords


  //no need of backtick as just a string
 val dfWithLongColName = df.withColumn(
   "This Long Column-Name",
   expr("ORIGIN_COUNTRY_NAME"))

//use backticks because we’re referencing column in an expression
  dfWithLongColName.selectExpr(
    "`This Long Column-Name`",
    "`This Long Column-Name` as `new col`")
    .show(2)

  // removing 1 column
  df.drop("ORIGIN_COUNTRY_NAME").columns

  //removing multiple columns
  dfWithLongColName.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME")

  //type cast a column
  df.withColumn("count2", col("count").cast("long"))
//using where and filter
  df.filter(col("count") < 2).show(2)
  df.where("count < 2").show(2)


  //multiple filters [Replacement of AND In sql]
  df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia")
    .show(2)


  //Getting Unique Rows [similar to distinct in sql]
  df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()
  df.select("ORIGIN_COUNTRY_NAME").distinct().count()

//Random sample
val seed = 5
  val withReplacement = false
  val fraction = 0.5

  df.sample(withReplacement, fraction, seed).count()

  //Split Randoms used for ML
  val dataFrames = df.randomSplit(Array(0.25, 0.75), seed)
  dataFrames(0).count() > dataFrames(1).count() // False

  /**
    * UNION OPERATION
    */

  import org.apache.spark.sql.Row
  import spark.implicits._
  val schema = df.schema
  val newRows = Seq(
    Row("New Country", "Other Country", 5L),
    Row("New Country 2", "Other Country 3", 1L))
  val parallelizedRows = spark.sparkContext.parallelize(newRows)
  val newDF = spark.createDataFrame(parallelizedRows, schema)
  df.union(newDF).where("count = 1").where($"ORIGIN_COUNTRY_NAME" =!= "United States").show() //get all of them and we'll see our new rows at the en


  /**
    * SORT // ORDERBY
    */
  df.sort("count").show(5)
  df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
  df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)


  // Based on order
  import org.apache.spark.sql.functions.{desc, asc}
  df.orderBy(expr("count desc")).show(2)
  df.orderBy(desc("count"), asc("DEST_COUNTRY_NAME")).show(2)

//Sort within partition  desc
  spark.read.format("json").load("/home/anmols/Desktop/Spark_Certification_preperation/Spark-The-Definitive-Guide/data/flight-data/json/*-summary.json")
    .sortWithinPartitions(desc(("DEST_COUNTRY_NAME"))).show


  //limit
  df.limit(5).show()
  df.orderBy(expr("count desc")).limit(6).show()

  //REPARTITIONING

  df.rdd.getNumPartitions // 1

  df.repartition(5) //creates new df

  //repartitioning based on that column

  df.repartition(col("DEST_COUNTRY_NAME"))   //will create 200 partitions

  //also provide number of partitions
  df.repartition(5, col("DEST_COUNTRY_NAME"))

//coalesce
 df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)


  //collect to driver
//collect gets all data from the entire DataFrame, take selects the first N rows, and show prints out a number of rows nicely.
  val collectDF = df.limit(10)
  collectDF.take(5) // take works with an Integer count
  collectDF.show() // this prints it out nicely
  collectDF.show(5, false)
  collectDF.collect()
//or use toLocalIterator()
  collectDF.toLocalIterator()


}


