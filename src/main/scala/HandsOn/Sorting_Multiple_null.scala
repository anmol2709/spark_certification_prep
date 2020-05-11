package HandsOn

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Sorting_Multiple_null extends App {

  val conf= new SparkConf().setMaster("local[*]").setAppName("sort")

  val spark = SparkSession.builder().appName("sort").config(conf).getOrCreate()

  import spark.implicits._
val df1 = Seq((1,2,"y"),(2,2,"z"),(2,2,"s")).toDF("c","d","e")
val df2 = Seq((1,2),(7,4),(1,3)).toDF("a","b")
  val joinedDf=df1.join(df2,df1.col("c")===df2.col("a"),"left_outer")
//  joinedDf.show

  /**
    * sorting with nullfirstdesc
    */
  joinedDf.select("*").sort(desc_nulls_first("a"))
  /**
    * won't sort as no null
    */
  joinedDf.select("*").sort(asc_nulls_first("c"))

  /**
    * sorting on multiple columns
    * first on `c`[Int] then on `e`[string]
    */

  joinedDf.select("*").sort(asc("c"),asc("e")).show()



}
