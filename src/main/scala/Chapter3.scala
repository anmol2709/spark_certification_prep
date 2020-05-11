import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object Chapter3 extends App {
  val conf = new SparkConf().setMaster("local[*]").setAppName("Spark-Assignment-2")
  val sparkSession = SparkSession.builder
    .config(conf = conf)
    .appName("spark session example")
    .getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")
import sparkSession.implicits._

  val staticDataFrame = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true")
    .load("/home/anmols/Desktop/Spark_Certification_preperation/Spark-The-Definitive-Guide/data/retail-data/by-day/*.csv")



  val staticSchema = staticDataFrame.schema


  //STREAMING DATA THROUGH SPARK


  val streamingDataFrame = sparkSession.readStream.schema(staticSchema).option("maxFilesPerTrigger", 1).format("csv")
    .option("header", "true")
    .load("/home/anmols/Desktop/Spark_Certification_preperation/Spark-The-Definitive-Guide/data/retail-data/by-day/*.csv")

  val purchaseByCustomerPerHour = streamingDataFrame
    .selectExpr(
      "CustomerId",
      "(UnitPrice * Quantity) as total_cost",
      "InvoiceDate")
    .groupBy(
      $"CustomerId", window($"InvoiceDate", "1 day"))
    .sum("total_cost")

      //to print continously on console

  /*purchaseByCustomerPerHour.writeStream
    .format("console")
    .queryName("customer_purchases_2")
    .outputMode("complete")
    .start()*/


  //to write in memory
  purchaseByCustomerPerHour.writeStream
    .format("memory") // memory = store in-memory table
    .queryName("customer_purchases_2") // the name of the in-memory table
    .outputMode("complete") // complete = all the counts should be in the table
    .start()


  Thread.sleep(10000)
  //  purchaseByCustomerPerHour.createOrReplaceTempView("customer_purchases")
  sparkSession.sql("""
SELECT *
FROM customer_purchases_2
ORDER BY `sum(total_cost)` DESC
""")
    .show(5)

Thread.sleep(55000)
}
