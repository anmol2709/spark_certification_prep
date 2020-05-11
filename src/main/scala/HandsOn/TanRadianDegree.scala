package HandsOn

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object TanRadianDegree extends App{

  val config = new SparkConf().setMaster("local[*]").setAppName("app")
  val spark = SparkSession.builder().config(config).appName("app").getOrCreate()


  import spark.implicits._
  val df = Seq((1,2),(2,4)).toDF("radian","id")
  val dfradiantodegree= df.withColumn("degree",degrees(col("radian")))
  val dfdegreetoradian = dfradiantodegree.withColumn("radianNew",radians("degree"))
dfdegreetoradian.select("id","radian","degree","radianNew").withColumn("tan",tan("radian")).show()
//  dfdegreetoradian.select(tan("radian")).show

}
