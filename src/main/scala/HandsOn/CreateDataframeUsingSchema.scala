package HandsOn

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.columnar.ARRAY
import org.apache.spark.sql.types._
object CreateDataframeUsingSchema extends App{
  val conf = new SparkConf().setMaster("local[*]").setAppName("APIFunctions")

  val spark = SparkSession.builder
    .config(conf = conf)
    .appName("spark session example")
    .getOrCreate()
  import spark.implicits._


val manualSchema= StructType(
  List(
    StructField("id",IntegerType),
    StructField("array",ArrayType(IntegerType))
  )
)
  val rows: List[Row] = List(Row(1,Array(1)))
  val df= spark.createDataFrame(spark.sparkContext.parallelize(rows),manualSchema)


  println(df.schema)
  df.show()


  val manualSchema2=manualSchema.add(StructField("name",StringType))
  val rows2: List[Row] = List(Row(1,Array(1),"Name"))
  val df2=spark.createDataFrame(spark.sparkContext.parallelize(rows2),manualSchema2)


  println(df2.schema)
  df2.show()

//used for table creation ==> DDL
val manualSchema3= manualSchema2.toDDL

}
