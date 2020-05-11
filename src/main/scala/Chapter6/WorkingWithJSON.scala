package Chapter6

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object WorkingWithJSON extends App {

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
    * create a Json Column
    */
  val jsonDF = spark.range(1).selectExpr("""
'{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString""")


  /**
    * get_json_object :to inline query a JSON object
    */

  import org.apache.spark.sql.functions.{get_json_object, json_tuple}
  jsonDF.select(
    get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]") as "column",
    json_tuple(col("jsonString"), "myJSONKey")).show(2)
  /**
    * json_tuple : if the object has only one level of nesting:
    */

  jsonDF.selectExpr(
    "json_tuple(jsonString, '$.myJSONKey.myJSONValue[1]') as column").show(2)


  /**
    * Turn a StructType into a JSON string by using the to_json function:
    */

  import org.apache.spark.sql.functions.to_json
  df.selectExpr("(InvoiceNo, Description) as myStruct")
    .select(to_json(col("myStruct")))


  /**
    * This function also accepts a dictionary (map) of parameters that are the same as the JSON data
    * source. You can use the from_json function to parse this (or other JSON data) back in. This naturally
    * requires you to specify a schema, and optionally you can specify a map of options, as well:
    */


  import org.apache.spark.sql.functions.from_json
  import org.apache.spark.sql.types._

  val parseSchema = new StructType(Array(
    new StructField("InvoiceNo",StringType,true),
    new StructField("Description",StringType,true)))

  df.selectExpr("(InvoiceNo, Description) as myStruct")
    .select(to_json(col("myStruct")).alias("newJSON"))	//convert to json
    .select(from_json(col("newJSON"), parseSchema), col("newJSON")).show(2) //back to Struct






}