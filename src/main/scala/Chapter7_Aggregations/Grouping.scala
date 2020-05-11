package Chapter7_Aggregations

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Grouping extends App {

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
    * We will group by each unique invoice number and get the count of items on that invoice.
    * Note that this returns another DataFrame and is lazily performed.
    */

  df.groupBy("InvoiceNo", "CustomerId").count().show()


  /**
    * Grouping with Expressions
    */

  import org.apache.spark.sql.functions.count
  import org.apache.spark.sql.functions.expr
  df.groupBy("InvoiceNo").agg(
    count("Quantity").alias("quan"),    //using  it inside agg
    expr("count(Quantity)")).show()                 //using as  expression
}
