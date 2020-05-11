package HandsOn

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object GroupBy_Encoding extends App{
  val conf = new SparkConf().setMaster("local[*]").setAppName("APIFunctions")

  val spark = SparkSession.builder
    .config(conf = conf)
    .appName("spark session example")
    .getOrCreate()
  import spark.implicits._


  val df1= Seq((1,0,"name1"),(3,2,"name1"),(1,2,"name1"),(1,5,"name1"),(4,2,"name1")).toDF("id","stateid","name")

  df1.groupBy("id").count().show()


  /**
    * Using encode ==>
    * charset to be provided ==>'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16'
    */
  df1.select(encode(col("name"),"UTF-8")).show

}
