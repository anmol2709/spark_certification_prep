//
//import org.apache.spark.sql.functions.{col, window}
//
//object Extras {
//import spark.implicits._
//val conf = new SparkConf().setMaster("local[*]").setAppName("Spark-Assignment-2")
//
//  val spark = SparkSession.builder
//    .config(conf = conf)
//    .appName("spark session example")
//    .getOrCreate()
//
//  val staticDataFrame = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/home/anmols/Desktop/Spark_Certification_preperation/Spark-The-Definitive-Guide/data/retail-data/by-day/*.csv")
//
////  staticDataFrame.selectExpr("CustomerId","(UnitPrice * Quantity) as total_cost","InvoiceDate").groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 day")).sum("total_cost").show(5)
//  staticDataFrame.selectExpr("CustomerId","(UnitPrice * Quantity) as total_cost","InvoiceDate").groupBy($"CustomerId", window($"InvoiceDate", "1 day")).sum("total_cost").show(5)
//
//
//  val purchaseByCustomerPerHour = streamingDataFrame.selectExpr("CustomerId","(UnitPrice * Quantity) as total_cost","InvoiceDate").groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 day")).sum("total_cost")
//
//
//  purchaseByCustomerPerHour.writeStream.format("memory").queryName("customer_purchases").outputMode("complete").start()
//
//
//
//  val df1 = spark.read.format("json").load("/home/anmols/Desktop/Spark_Certification_preperation/Spark-The-Definitive-Guide/data/flight-data/json/2015-summary.json").columns
//
//
//  val streamingDataFrame = spark.readStream
//    .option("inferSchema",true)
//    .option("maxFilesPerTrigger", 1)
//    .format("csv")
//    .option("header", "true")
//    .load("/home/anmols/Desktop/Spark_Certification_preperation/Spark-The-Definitive-Guide/data/retail-data/by-day/*.csv")
//}
