package HandsOn

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object PlayingWithUdf extends App{

  val config = new SparkConf().setMaster("local[*]").setAppName("app")

  val spark = SparkSession.builder().config(config).appName("app").getOrCreate()

  import spark.implicits._

  def doubleUdf(num:Int)= num*num
  val dfLatest= Seq((1,2),(3,4)).toDF("a","b")
  dfLatest.createOrReplaceTempView("dfLatest")
  /**
    * using udf in sql query
    */
  //from spark.udf
  spark.udf.register("doubleudf",doubleUdf(_:Int):Int)

  spark.sql("""select doubleudf(a) as dbludf from dfLatest""").show()

  /**
    * using in select
    */
//from sql.functions._
  val doubUDF=udf(doubleUdf(_:Int):Int)
  dfLatest.select(col("a"),doubUDF(col("a"))).show()

}
