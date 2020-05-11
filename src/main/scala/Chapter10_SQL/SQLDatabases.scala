package Chapter10_SQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SQLDatabases extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("APIFunctions")

  val spark = SparkSession.builder
    .config(conf = conf)
    .appName("spark session example")
    .getOrCreate()
  import spark.implicits._

  /**
    * Create DB
    */
  val createDB= """CREATE DATABASE some_db"""
  spark.sql(createDB)

  /**
    * show db
    */
  val showDB= """SHOW DATABASES"""
  spark.sql(showDB)

  /**
    * Setting the db
    */
  val useDb= """USE some_db"""


  /**
    * show current db
    */
  val currentDb = """SELECT current_database()"""
  spark.sql("use some_db") //changing current db to some_db
  spark.sql(currentDb).show()
  /**
    *Drop database
    */
  val dropDb= """DROP DATABASE IF EXISTS some_db;"""
}
