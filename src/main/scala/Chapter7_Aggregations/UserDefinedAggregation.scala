package Chapter7_Aggregations

/**
  *
  * TO
  * BE
  * DONE
  * LATER
  *
  */

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class BoolAnd extends UserDefinedAggregateFunction {
  def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", BooleanType) :: Nil)

  def bufferSchema: StructType = StructType(
    StructField("result", BooleanType) :: Nil
  )

  def dataType: DataType = BooleanType

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = true
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Boolean](0) && input.getAs[Boolean](0)
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Boolean](0) && buffer2.getAs[Boolean](0)
  }

  def evaluate(buffer: Row): Any = {
    buffer(0)
  }
}


object UserDefinedAggregation extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("APIFunctions")

  val spark = SparkSession.builder
    .config(conf = conf)
    .appName("spark session example")
    .getOrCreate()


  // Now, we simply instantiate our class and/or register it as a function:
  // in Scala
  val ba = new BoolAnd
  spark.udf.register("booland", ba)

  import org.apache.spark.sql.functions._

  spark.range(1)
    .selectExpr("explode(array(TRUE, TRUE, TRUE)) as t").selectExpr("explode(array(TRUE, FALSE, TRUE)) as f", "t")
    .select(ba(col("t")), expr("booland(f)"))
    .show()
}
