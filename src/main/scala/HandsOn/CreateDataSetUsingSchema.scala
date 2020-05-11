package HandsOn


import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object CreateDataSetUsingSchema extends App{

  val conf = new SparkConf().setMaster("local[*]").setAppName("APIFunctions")

  val spark = SparkSession.builder
    .config(conf = conf)
    .appName("spark session example")
    .getOrCreate()
  /**
    * Create dataset using schema
    */
import spark.implicits._
  case class Eng(id:Int,name:String)
  val mschema= new StructType(Array(
    StructField("a",IntegerType),
    StructField("b",StringType)
  ))
  val df= spark.createDataFrame(spark.sparkContext.parallelize(List(Row((1,2)))),mschema)
  val a= List(Eng(1,"a"))
  val dataset= spark.createDataset(a)
  dataset.printSchema()
  dataset.show()
}
