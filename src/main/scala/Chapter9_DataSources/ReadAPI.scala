package Chapter9_DataSources

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ReadAPI extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("APIFunctions")

  val spark = SparkSession.builder
  .config(conf = conf)
  .appName("spark session example")
  .getOrCreate()
import spark.implicits._

  /**
    * GENERIC READ API
    * DataframeReader[spark.read].format(...).option("key", "value").schema(...).load()
    */

//Example:
  spark.read.format("csv")
    .option("mode", "FAILFAST")
    .option("inferSchema", "true")
    .option("path", "path/to/file(s)") //change this
    .schema("SomeSchema") //this also
    .load()


  val person = Seq(
    (0, "Bill Chambers", 0, Seq(100)),
    (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
    (2, "Michael Armbrust", 1, Seq(250, 100)))
    .toDF("id", "name", "graduate_program", "spark_status")


  /**
    * Write API Structure
    * DataFrameWriter.format(...).option(...).partitionBy(...).bucketBy(...).sortBy(
    * ...).save()
    *
    * we specify three values:
    * - the format,
    * - a series of options, //At a minimum, you must supply a path
    * -the save mode.
    */

  person.write.format("csv")
    .option("mode", "OVERWRITE").option("dateFormat", "yyyy-MM-dd")
    .option("path", "path/to/file(s)")
    .save()

}

