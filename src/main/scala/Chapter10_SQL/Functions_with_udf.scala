package Chapter10_SQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Functions_with_udf extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("APIFunctions")

  val spark = SparkSession.builder
    .config(conf = conf)
    .appName("spark session example")
    .getOrCreate()
  import spark.implicits._

  /**
    * see a list of functions in Spark SQL
    */
  val showFunc= """SHOW FUNCTIONS"""

  /**
    * system functions (i.e.,
    * those built into Spark)
    */

  val showSystemFunc= """SHOW SYSTEM FUNCTIONS"""

  /**
    * SHOW USER FUNCTIONS
    */

  val showUserFunc= """SHOW USER FUNCTIONS"""

  /**
    * Wildcards
    */

  val wildcard= """SHOW FUNCTIONS "s*";"""
  val wildcardLike= """SHOW FUNCTIONS LIKE "collect*";"""


  val query2 = """CREATE TABLE flights (
                DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)
                USING JSON OPTIONS (path '/home/anmols/Desktop/Spark_Certification_preperation/Spark-The-Definitive-Guide/data/flight-data/json/2015-summary.json')

                """
  spark.sql(query2)
  /**
    * UDF ==> user defined function
    * @param number
    * @return
    */
  def power3(number:Double):Double = number * number * number


      //using register
  spark.udf.register("power3", power3(_:Double):Double)
  val query = """SELECT count, power3(count) FROM flights"""
  spark.sql(query).show(5)

  import  org.apache.spark.sql.functions._

  //usinf df on table


  val df2= spark.sql("""SELECT * FROM flights""")
  val power3udf2 = udf(power3(_:Double):Double)
  df2.select(col("count"),power3udf2(col("count"))).show(5)




  //using udf on range
  val udfExampleDF = spark.range(5).toDF("num")
  val power3udf = udf(power3(_:Double):Double)
  udfExampleDF.select($"num",power3udf(col("num")))


}
