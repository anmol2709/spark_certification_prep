package Chapter8_Joins

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object ChallengesWithJoins extends App {

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
    * Join on complex types
    * checking logic for array
    *
    *
    * will print all rows if spark_status[array coloumn] contains the id of the sparkstatus table
    */
person.withColumnRenamed("id", "personId")
  .join(sparkStatus, expr("array_contains(spark_status, id)")).show()  //works fine
//As the person.spark_status is an array. It will check if the array contains the values for spark_status.id ⇒ if yes. ⇒ row added
  //Else dropped

  /**
    * ----------------------------------------------------------------------------------------------------
    */


  /**
    * DUPLICATE NAME ISSUE WITH JOINS
    *
    */


  val gradProgramDupe = graduateProgram.withColumnRenamed("id", "graduate_program")

  //condition
  val joinExpr = gradProgramDupe.col("graduate_program") === person.col(
    "graduate_program")

  person.join(gradProgramDupe, joinExpr).show() //works fine as printing all

  person.join(gradProgramDupe, joinExpr).select("graduate_program").show() //gives error as 2 columns named graduate_program


  /**
    * ----------------------------------------------------------------------------------------------------
    */

  /**
    * FIXES FOR DUPLICATE NAMES
    */

  /**
    * 1. JOINING ON A COLUMN Name - wll remove duplicate
    */

  person.join(gradProgramDupe,"graduate_program").select("graduate_program").show()
  /**
    * 2. Dropping the column after the join
    */

  person.join(gradProgramDupe, joinExpr).drop(person.col("graduate_program"))
    .select("graduate_program").show()

  /**
    * 3. Renaming the column before the join
    */

  val gradProgram3 = graduateProgram.withColumnRenamed("id", "grad_id")
  val joinExpr1 = person.col("graduate_program") === gradProgram3.col("grad_id")
  person.join(gradProgram3, joinExpr1).show()
  /**
    * ----------------------------------------------------------------------------------------------------
    */


  /**
    * Broadcast join explicitly
    *big table to small
    * APJOIN, BROADCAST, and BROADCASTJOIN all do the same thing
    */
  import org.apache.spark.sql.functions.broadcast
  val joinExprBroad = person.col("graduate_program") === graduateProgram.col("id")
  person.join(broadcast(graduateProgram), joinExprBroad).explain()   //.explain will give details

 // in Sql
// giving hints in sql to optimizer
spark.sql("SELECT /*+ MAPJOIN(graduateProgram) */ * FROM person JOIN graduateProgram " +
  "ON person.graduate_program = graduateProgram.id")
}


