package HandsOn

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object PlayingWithPartitions extends App{
  val config = new SparkConf().setMaster("local[*]").setAppName("app")

  val spark = SparkSession.builder().config(config).appName("app").getOrCreate()

  import spark.implicits._

  def doubleUdf(num:Int)= num*num
  val dfLatest= Seq((1,2),(3,4)).toDF("a","b")


  println("initial ==> " + dfLatest.rdd.getNumPartitions)

  /**
    * coalesce ==> won't increase [ON COALESCE]
    */
  val newDfCoal= dfLatest.coalesce(5)
  println("after coalesce increase ==> " + newDfCoal.rdd.getNumPartitions)

  /**
    * repartitioned ==> will increase
    */
  val newDfRepart= dfLatest.repartition(5)
  println("after repartition==> " + newDfRepart.rdd.getNumPartitions)


  /**
    * coalesce decrease ==> will decrease
    */

  val dfCoalDec= newDfRepart.coalesce(2)
println("coalesce decrease" + dfCoalDec.rdd.getNumPartitions)
}
