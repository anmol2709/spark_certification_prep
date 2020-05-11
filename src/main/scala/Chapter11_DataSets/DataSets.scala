package Chapter11_DataSets

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset, SparkSession}

object DataSets extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("APIFunctions")

  val spark = SparkSession.builder
    .config(conf = conf)
    .appName("spark session example")
    .getOrCreate()
  import spark.implicits._

  /**
    * Using Dataset
    * @param DEST_COUNTRY_NAME
    * @param ORIGIN_COUNTRY_NAME
    * @param count
    */
  case class Flight(DEST_COUNTRY_NAME: String,
                    ORIGIN_COUNTRY_NAME: String, count: BigInt)
  //creating dataframe
  val flightsDF = spark.read
    .parquet("/home/anmols/Desktop/Spark_Certification_preperation/Spark-The-Definitive-Guide/data/flight-data/parquet/2010-summary.parquet/")
//converting to dataset
  val flights = flightsDF.as[Flight]


  /**
    * =====================Actions=====================
    */
  flights.show(2)

//can access the same as a case class
 flights.first.DEST_COUNTRY_NAME//getting field of case class
 // res2: String = United States

  /**
    * =====================Transformations=====================
    */
  /**
    * Filtering
    */
  //not a udf // but a generic function. //this function does not need to execute in Spark code at all
  //a method to check origin same as dest
  def originIsDestination(flight_row: Flight): Boolean = {
     flight_row.ORIGIN_COUNTRY_NAME == flight_row.DEST_COUNTRY_NAME}

  flights.filter(originIsDestination(_)).first()
  //returns res3: Flight = Flight(United States,United States,348113)

  //collecting as array
  flights.collect().filter(flight_row => originIsDestination(flight_row))
//returns res5: Array[Flight] = Array(Flight(United States,United States,348113))

  /**
    * Mapping
    */

  val destinations: Dataset[String] = flights.map(f => f.DEST_COUNTRY_NAME)
  val destinationsDf: DataFrame = flights.map(f => f.DEST_COUNTRY_NAME).toDF()
  val localDestinations = destinations.take(5) //returns array[string]


  /**
    * Joins
    */
  case class FlightMetadata(count: BigInt, randomData: BigInt)
  //joinWith is roughly equal to a co-group
  //(in RDD terminology) and you basically end up with two nested Datasets inside of one

  val flightsMeta = spark.range(500).map(x => (x, scala.util.Random.nextLong))
    .withColumnRenamed("_1", "count").withColumnRenamed("_2", "randomData")
    .as[FlightMetadata]

  val flights2: Dataset[(Flight, FlightMetadata)] =  flights.joinWith(flightsMeta,flights.col("count") === flightsMeta.col("count"))
  val flights2joinusingdf: DataFrame =flights.join(flightsMeta, Seq("count"))
  /**
    * returns
    * +--------------------+--------------------+
    * |                  _1|                  _2|
    * +--------------------+--------------------+
    * |[United States,Ro...|[1,54338557513382...|
    * |[United States,Ir...|[264,393066822906...|
    * |[United States,In...|[69,2036335059084...|
    * |[Egypt,United Sta...|[24,6496766299111...|
    * |[Equatorial Guine...|[1,54338557513382...|
    * |[United States,Si...|[25,1542697622181...|
    * |[United States,Gr...|[54,7354190495827...|
    * |[Costa Rica,Unite...|[477,572366744888...|
    */
  val usingDf = flights2.selectExpr("_1.DEST_COUNTRY_NAME")

  val usingDs =flights2.map(x=>x._1.DEST_COUNTRY_NAME).toDF()

  val ret: Array[(Flight, FlightMetadata)] = flights2.take(2)


  //can also join df and ds
  val flights3 = flights.join(flightsMeta.toDF(), Seq("count"))

  /**
    * Grouping and Aggregations
    */
  val returnDf: DataFrame =flights.groupBy("DEST_COUNTRY_NAME").count()
  val returnDs: Dataset[(String, Long)] =flights.groupByKey(x => x.DEST_COUNTRY_NAME).count()

  flights.groupByKey(x => x.DEST_COUNTRY_NAME).count().explain
  /**
    * == Physical Plan ==
    * *HashAggregate(keys=[value#1396], functions=[count(1)])
    * +- Exchange hashpartitioning(value#1396, 200)
    * +- *HashAggregate(keys=[value#1396], functions=[partial_count(1)])
    * +- *Project [value#1396]
    * +- AppendColumns <function1>, newInstance(class ...
    * [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, ...
    * +- *FileScan parquet [D...
    *
    */

  def grpSum(countryName:String, values: Iterator[Flight]) = {
    values.dropWhile(_.count < 5).map(x => (countryName, x))
  }
  flights.groupByKey(x => x.DEST_COUNTRY_NAME).flatMapGroups(grpSum).show(5)

//  We can even create new manipulations and define how groups should be reduced:
  def sum2(left:Flight, right:Flight) = {
    Flight(left.DEST_COUNTRY_NAME, null, left.count + right.count)
  }

  flights.groupByKey(x => x.DEST_COUNTRY_NAME).reduceGroups((l, r) => sum2(l, r))
    .take(5)


  //explaining logic
  flights.groupBy("DEST_COUNTRY_NAME").count().explain
  /**
    * == Physical Plan ==
    * *HashAggregate(keys=[DEST_COUNTRY_NAME#1308], functions=[count(1)])
    * +- Exchange hashpartitioning(DEST_COUNTRY_NAME#1308, 200)
    * +- *HashAggregate(keys=[DEST_COUNTRY_NAME#1308], functions=[partial_count(1)])
    * +- *FileScan parquet [DEST_COUNTRY_NAME#1308] Batched: tru...
    */
}
