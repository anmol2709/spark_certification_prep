package HandsOn

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object JoinPractice extends App{
  val conf = new SparkConf().setMaster("local[*]").setAppName("APIFunctions")

  val spark = SparkSession.builder
    .config(conf = conf)
    .appName("spark session example")
    .getOrCreate()
  import spark.implicits._

  val person = Seq(
    (0, "Bill Chambers", 0, Seq(100)),
    (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
    (2, "Michael Armbrust", 1, Seq(250, 100)))
    .toDF("id", "name", "graduate_program", "spark_status")
  person.createOrReplaceTempView("person") //registering these tables as SQL views to use in spark.sql

  val graduateProgram = Seq(
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley"))
    .toDF("id", "degree", "department", "school")
  graduateProgram.createOrReplaceTempView("graduateProgram")

  val sparkStatus = Seq(
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor"))
    .toDF("id", "status")
  sparkStatus.createOrReplaceTempView("sparkStatus")
  val joinexpr= person.col("graduate_program")===graduateProgram.col("id")
  val newDf=person.join(graduateProgram,joinexpr,"inner")
newDf.show(5)

//type of join in chapter 8

  import org.apache.spark.sql.functions
}
