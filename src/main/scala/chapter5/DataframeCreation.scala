package chapter5

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
object DataframeCreation extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("Spark-Assignment-2")

  val spark = SparkSession.builder
    .config(conf = conf)
    .appName("spark session example")
    .getOrCreate()

      val df: DataFrame = spark.read.format("json").load("/home/anmols/Desktop/Spark_Certification_preperation/Spark-The-Definitive-Guide/data/flight-data/json/2015-summary.json")
  df.createOrReplaceTempView("dfTable")


  //Creating a Dataframe on the fly

  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
  val myManualSchema = new StructType(Array(
    new StructField("some", StringType, true),
    new StructField("col", StringType, true),
    new StructField("names", LongType, false)))
  val myRows = Seq(Row("Hello", null, 1L))
  val myRDD = spark.sparkContext.parallelize(myRows)
  val myDf = spark.createDataFrame(myRDD, myManualSchema)
  myDf.show()

}
