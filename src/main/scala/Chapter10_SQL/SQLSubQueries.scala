package Chapter10_SQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SQLSubQueries extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("APIFunctions")

  val spark = SparkSession.builder
    .config(conf = conf)
    .appName("spark session example")
    .getOrCreate()
  import spark.implicits._

  /**
    * SUBQUERIES ==> queries within other queries
    *
    * TYPES::::::::::::::
    * Correlated subqueries use some information from the outer scope
    * of the query in order to supplement
    * information in the subquery.
    *
    * Uncorrelated subqueries include no information from the outer scope.
    *
    * Predicate subqueries, which allow for filtering based on values.
    *
    */


  /**
    * Uncorrelated predicate subqueries
    * This query is uncorrelated because it does not include any information from the outer scope of the
    * query. It’s a query that you can run on its own.
    */
//dummy query
  val query1 = """SELECT dest_country_name FROM flights
                 |GROUP BY dest_country_name ORDER BY sum(count) DESC LIMIT 5"""


  //using query1 as subquery
  val superquery = """SELECT * FROM flights
                     |WHERE origin_country_name IN (SELECT dest_country_name FROM flights
                     |GROUP BY dest_country_name ORDER BY sum(count) DESC LIMIT 5)"""


  /**
    *Correlated predicate subqueries
    * allow you to use information from the outer scope in your inner
    * query
    */

  val exampleQuery ="""SELECT * FROM flights f1
                      |WHERE EXISTS (SELECT 1 FROM flights f2
                      |WHERE f1.dest_country_name = f2.origin_country_name)
                      |AND EXISTS (SELECT 1 FROM flights f2
                      |WHERE f2.dest_country_name = f1.origin_country_name)"""


  /***
    * Uncorrelated scalar queries
    * to include the maximum value as its own column from
    * the entire counts dataset
    */
val getMax = """SELECT *, (SELECT max(count) FROM flights) AS maximum FROM flights"""
}
