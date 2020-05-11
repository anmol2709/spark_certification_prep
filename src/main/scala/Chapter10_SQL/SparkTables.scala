package Chapter10_SQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkTables extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("APIFunctions")

  val spark = SparkSession.builder
    .config(conf = conf)
    .appName("spark session example")
    .getOrCreate()
  import spark.implicits._

  /**
    * read in the flight data from json
    */
  val query = """CREATE TABLE flights (
                |DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)
                |USING JSON OPTIONS (path '/home/anmols/Desktop/Spark_Certification_preperation/Spark-The-Definitive-Guide/data/flight-data/json/2015-summary.json')
                |
                |"""
spark.sql(query)

  /**
    * table from a query as well:
    * if not exists
    */
  val query1 =    """CREATE TABLE  IF NOT EXISTS flights_from_select USING parquet AS SELECT * FROM flights"""
spark.sql(query1)

  /**
    * Control the layout of the data by writing out a partitioned dataset,
    */
  val partitionedQuery = """CREATE TABLE partitioned_flights USING parquet PARTITIONED BY (DEST_COUNTRY_NAME) AS SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count FROM flights LIMIT"""

  spark.sql(partitionedQuery)


  /**
    * INSERT INTO TABLE
    * Insertions follow the standard SQL syntax:
    */

  val insertQuery = """INSERT INTO flights_from_select SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count FROM flights LIMIT 20"""

  spark.sql(insertQuery)
  /**
    * if you want to write only into a certain partition.
    */
  val partitionInsertQuery ="""INSERT INTO partitioned_flights
                              |PARTITION (DEST_COUNTRY_NAME="UNITED STATES")
                              |SELECT count, ORIGIN_COUNTRY_NAME FROM flights
                              |WHERE DEST_COUNTRY_NAME='UNITED STATES' LIMIT 12"""
  spark.sql(partitionInsertQuery)


  /**
    *Describing Table Metadata
    */
val descQuery = """DESCRIBE TABLE flights_csv"""

  /**
    * partitioning scheme for the data
    */
  val partitionScheme= """SHOW PARTITIONS partitioned_flights"""

  /**
    * Refreshing Table Metadata
    * to get most recent set of data.
    * REFRESH TABLE refreshes all cached entries (essentially, files) associated with the table.
    * If the table were previously cached, it would be cached lazily the next time it is scanned:
    */
val refreshMetadata ="""REFRESH table partitioned_flights"""
  /**
    * REPAIR TABLE, which refreshes the partitions maintained in the catalog
    * collecting new partition information
    */
  val repairMetadata = """MSCK REPAIR TABLE partitioned_flights"""


  /**
    * Dropping Tables
    * deletes table + data
    * Note: If you are dropping an unmanaged table (e.g., hive_flights), no data will be removed but you will
    * no longer be able to refer to this data by the table name.
    */
  val dropTables = """DROP TABLE flights_csv;""" //fails if no table exists
  val dropTablesIFNOTEXISTS = """DROP TABLE IF EXISTS flights_csv"""
  /**
    * Caching table
    *
    */
   val cacheTable="""CACHE TABLE flights"""
  /**
    * Uncache
    */
  val uncacheTable="""UNCACHE TABLE FLIGHTS"""


  /**
    * Creating Views
    */

val createView=  """CREATE VIEW just_usa_view AS
    |SELECT * FROM flights WHERE dest_country_name = 'United States'"""

  /**
    * temporary views that are available only during the current session and are
    * not registered to a database:
    */
  val tempView= """CREATE TEMP VIEW just_usa_view_temp AS
                  |SELECT * FROM flights WHERE dest_country_name = 'United States'"""

  /**
    * Global temp views are resolved regardless of database and are
    * viewable across the entire Spark application, but they are removed at the end of the session
    */

  val globalTempView= """CREATE GLOBAL TEMP VIEW just_usa_global_view_temp AS
                        |SELECT * FROM flights WHERE dest_country_name = 'United States'"""


  /**
    * overwite a view if one already exists
    */
  val createOrReplaceTempView = """CREATE OR REPLACE TEMP VIEW just_usa_view_temp AS
                                  |SELECT * FROM flights WHERE dest_country_name = 'United States'"""


  /**
    * DF vs SQL in explain
    */
  val flights = spark.read.format("json")
    .load("/data/flight-data/json/2015-summary.json")
  val just_usa_df = flights.where("dest_country_name = 'United States'")
  just_usa_df.selectExpr("*").explain



  val explainQuery ="""EXPLAIN SELECT * FROM flights WHERE dest_country_name = 'United States'"""

  /**
    * Dropping views
    */
  val dropView=   """DROP VIEW IF EXISTS just_usa_view;"""

}


