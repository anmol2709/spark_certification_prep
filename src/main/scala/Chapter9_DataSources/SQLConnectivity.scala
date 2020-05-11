package Chapter9_DataSources

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StructField, StructType}

object SQLConnectivity extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("APIFunctions")

  val spark = SparkSession.builder
    .config(conf = conf)
    .appName("spark session example")
    .getOrCreate()
  import spark.implicits._


  val driver = "org.sqlite.JDBC"
  val path = "/data/flight-data/jdbc/my-sqlite.db"
  val url = s"jdbc:sqlite:/${path}"
  val tablename = "flight_info"

  //Test connection

  import java.sql.DriverManager
  val connection = DriverManager.getConnection(url)
  connection.isClosed()
  connection.close()

  /**
    * DEMO READ POSTGRES :
    */

  val pgDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://database_server")
    .option("dbtable", "schema.tablename")
    .option("user", "username")
    .option("password","my-secret-password")
    .load()



  //Read DF
  val dbDataFrame = spark.read.format("jdbc").option("url", url)
    .option("dbtable", tablename).option("driver", driver).load()


  /**
    * Spark makes a best-effort attempt to filter data in the database itself before creating the
    * DataFrame.
    *
    * >>>>>  selects
    * only the relevant column name from the table
    **/

  dbDataFrame.select("DEST_COUNTRY_NAME").distinct().explain

  /**
    * The ability to specify a maximum number of partitions to allow you to limit how much you are reading and writing in parallel:
    *
    *
    */
  val dbDataFrame1 = spark.read.format("jdbc")
   .option("url", url).option("dbtable", tablename).option("driver", driver)
   .option("numPartitions", 10).load()


  /**
    * Setting predicate
    * We could filter these down and have them pushed into the database, but we can also go further by having them arrive in their own partitions in Spark.We do that by specifying a list of predicates when we create the data source:
    * //filter out the necessary items first and using these as predicates so as to provide them  their own partitions
    */

  val props = new java.util.Properties
  props.setProperty("driver", "org.sqlite.JDBC")
  val predicates = Array(
    "DEST_COUNTRY_NAME = 'Sweden' OR ORIGIN_COUNTRY_NAME = 'Sweden'",
    "DEST_COUNTRY_NAME = 'Anguilla' OR ORIGIN_COUNTRY_NAME = 'Anguilla'")
  spark.read.jdbc(url, tablename, predicates, props).show()
  spark.read.jdbc(url, tablename, predicates, props).rdd.getNumPartitions

  val myManualSchema = new StructType(Array(
    new StructField("DEST_COUNTRY_NAME", LongType, true),  //giving long type
    new StructField("ORIGIN_COUNTRY_NAME", LongType, true),
    new StructField("count", LongType, false) ))


  val csvFile = spark.read.format("csv")
    .option("header", "true").option("mode", "FAILFAST").schema(myManualSchema)
    .load("/home/anmols/Desktop/Spark_Certification_preperation/Spark-The-Definitive-Guide/data/flight-data/csv/2010-summary.csv")

  /**
    * Writing to SQL Databases
    */

  val newPath = "jdbc:sqlite://tmp/my-sqlite.db"
  csvFile.write.mode("overwrite").jdbc(newPath, tablename, props)


  //Results:
  spark.read.jdbc(newPath, tablename, props).count() //255

  /**
    * Writing to SQL Databases ==> Append
    */
  csvFile.write.mode("append").jdbc(newPath, tablename, props)
  //Result  ⇒ count increases
  spark.read.jdbc(newPath, tablename, props).count()// 765

}
