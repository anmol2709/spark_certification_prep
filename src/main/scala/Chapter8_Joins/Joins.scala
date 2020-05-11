package Chapter8_Joins

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Joins extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("APIFunctions")

  val spark = SparkSession.builder
  .config(conf = conf)
  .appName("spark session example")
  .getOrCreate()

  import spark.implicits._

  /**
    * Creating dataset
    */

  val person = Seq(
    (0, "Bill Chambers", 0, Seq(100)),
    (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
    (2, "Michael Armbrust", 1, Seq(250, 100)))
    .toDF("id", "name", "graduate_program", "spark_status")
  person.createOrReplaceTempView("person") //registering these tables as SQL views to use in spark.sql

  val graduateProgram = Seq(
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley"))
    .toDF("id", "degree", "department", "school")
  graduateProgram.createOrReplaceTempView("graduateProgram")

  val sparkStatus = Seq(
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor"))
    .toDF("id", "status")
  sparkStatus.createOrReplaceTempView("sparkStatus")

  /**
    * Inner Joins
    */

//correct join
  val joinExpression = person.col("graduate_program") === graduateProgram.col("id")

//Keys that do not exist in both DataFrames will not show in the resulting DataFrame
  val wrongJoinExpression = person.col("name") === graduateProgram.col("school")

  //=============Joining the dataframes===========
  person.join(graduateProgram,joinExpression).show()

  //using spark sql
  spark.sql("SELECT * FROM person JOIN graduateProgram ON person.graduate_program = graduateProgram.id").show

  //can also provide type of join
  person.join(graduateProgram, joinExpression,"inner").show()


  /**
    * OUTER JOIN
    * Outer joins evaluate the keys in both of the DataFrames or tables and includes (and joins together) the rows that evaluate to true or false.
    *
    * If there is no equivalent row in either the left or right DataFrame, Spark will insert null:
    */

  val joinType = "outer"

  person.join(graduateProgram, joinExpression, joinType).show()


  /**
    * Left Outer Joins
    * Left outer joins evaluate the keys in both of the DataFrames or tables and includes all rows from the
    * left DataFrame as well as any rows in the right DataFrame that have a match in the left DataFrame. If
    * there is no equivalent row in the right DataFrame, Spark will insert null:
    */

  graduateProgram.join(person, joinExpression, "left_outer").show()

  /**
    * Right Outer Joins
    * Right outer joins evaluate the keys in both of the DataFrames or tables and includes all rows from the
    * right DataFrame as well as any rows in the left DataFrame that have a match in the right DataFrame.
    * If there is no equivalent row in the left DataFrame, Spark will insert null:
    * */

  graduateProgram.join(person, joinExpression, "right_outer").show()


  /**
    * LEFT SEMI JOIN
    * They do not actually include any values from the right DataFrame.
    * They only compare values to see if the value exists in the second DataFrame. If
    * the value does exist, those rows will be kept in the result, even if there are duplicate keys in the left DataFrame.
    *
    * //if exist in right consider only then (just the left will be printed) else ignore[Like a filter]
    */

  val joinType_leftSemi = "left_semi"

  graduateProgram.join(person, joinExpression, joinType_leftSemi).show()

  /**
    * LEFT ANTI JOIN
    * Left anti joins are the opposite of left semi joins. Like left semi joins, they do not actually include any values from the right DataFrame.
    * They only compare values to see if the value exists in the second DataFrame. However, rather than keeping the values that exist in the second DataFrame,
    * they keep only the values that do not have a corresponding key in the second DataFrame.
    */
  graduateProgram.join(person, joinExpression, "left_anti").show()


  /**
    * NATURAL JOINS  --- use with caution
    * Natural joins make implicit guesses at the columns on which you would like to join. It finds matching
    * columns and returns the results. Left, right, and outer natural joins are all supported.
    */
  spark.sql("SELECT * FROM graduateProgram NATURAL JOIN person")


  /**
    * CROSS JOIN
    * Cross joins will join every single row in the left DataFrame to ever single row in the right DataFrame.
    *
    * NOTE : If you have 1,000 rows in each DataFrame, the cross-join of these will result in 1,000,000 (1,000 x 1,000) rows.
    */




//will return inner join as predicate 'joinexpression' given
  graduateProgram.join(person, joinExpression, "cross").show()


 //Actual cross join ==> n*n rows
  person.crossJoin(graduateProgram).show() //OR
  spark.sql("SELECT * FROM graduateProgram CROSS JOIN person").show //same
}

