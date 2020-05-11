package Chapter10_SQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SQLComplexTypes extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("APIFunctions")

  val spark = SparkSession.builder
    .config(conf = conf)
    .appName("spark session example")
    .getOrCreate()
  import spark.implicits._

  /**
    * =============================STRUCTS=============================
    * provide a way of creating or querying nested data in Spark
    * To create one, you simply need to wrap a set of columns (or expressions) in parentheses :::::
    * //something like a tuple
    */

  val createStructView = """CREATE VIEW IF NOT EXISTS nested_data AS
                     |SELECT (DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME) as country, count FROM flights
                     |"""

  /**
    * Query
    */

  //normal
val selectNormal=  """SELECT * FROM nested_data"""
//subcolumns
val selectsubcolumn=  """SELECT country.*, count FROM nested_data"""

 /**
    * =============================Lists=============================
   */

  /**
    * Create List
    */
val createList ="""SELECT DEST_COUNTRY_NAME as new_name, collect_list(count) as flight_counts,
                  |collect_set(ORIGIN_COUNTRY_NAME) as origin_set
                  |FROM flights GROUP BY DEST_COUNTRY_NAME"""

  //manual creation
  val manualCreation ="""SELECT DEST_COUNTRY_NAME, ARRAY(1, 2, 3) FROM flights"""

  /**
    * query lists by position
    */
 val queryByPosition ="""SELECT DEST_COUNTRY_NAME as new_name, collect_list(count)[0]
                       |FROM flights GROUP BY DEST_COUNTRY_NAME"""


  /**
    * using explode to convert an array back into rows
    */
  //creating new view
  val tempView= """CREATE OR REPLACE TEMP VIEW flights_agg AS
                  |SELECT DEST_COUNTRY_NAME, collect_list(count) as collected_counts
                  |FROM flights GROUP BY DEST_COUNTRY_NAME"""

  /**
    * one row in our result for every value in the array. The
    * DEST_COUNTRY_NAME will duplicate for every value in the array, performing the exact opposite of the
    * original collect and returning us to the original DataFrame:
    */
  val explodeToRows= """SELECT explode(collected_counts), DEST_COUNTRY_NAME FROM flights_agg"""
}
