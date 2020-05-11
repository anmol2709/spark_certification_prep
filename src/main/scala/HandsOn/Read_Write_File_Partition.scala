package HandsOn

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Read_Write_File_Partition extends App{

  val conf = new SparkConf().setMaster("local[*]").setAppName("APIFunctions")

  val spark = SparkSession.builder
    .config(conf = conf)
    .appName("spark session example")
    .getOrCreate()
  import spark.implicits._
  val df = spark.read.csv("/home/anmols/Desktop/Spark_Certification_preperation/Spark-The-Definitive-Guide/data/retail-data/by-day/2010-12-01.csv")

  println(">>>"+df.rdd.getNumPartitions)

  val newDF= df.repartition(5)
  println("<><,<<<<<<<<<<<<<<<<<<"+newDF.rdd.getNumPartitions)

  newDF.write.csv("/home/anmols/new")
}
