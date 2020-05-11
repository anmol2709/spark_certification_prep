package Chapter10_SQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SqlSelect extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("APIFunctions")

  val spark = SparkSession.builder
    .config(conf = conf)
    .appName("spark session example")
    .getOrCreate()
  import spark.implicits._

  /**
    * SELECT [ALL|DISTINCT] named_expression[, named_expression, ...]
    * FROM relation[, relation, ...][lateral_view[, lateral_view, ...]]
    * [WHERE boolean_expression]
    * [aggregation [HAVING boolean_expression]]
    * [ORDER BY sort_expressions]
    * [CLUSTER BY expressions]
    * [DISTRIBUTE BY expressions]
    * [SORT BY sort_expressions]
    * [WINDOW named_window[, WINDOW named_window, ...]]
    * [LIMIT num_rows]
    * named_expression:
    * : expression [AS alias]
    * relation:
    * | join_relation
    * | (table_name|query|relation) [sample] [AS alias]
    * : VALUES (expressions)[, (expressions), ...]
    * [AS (column_name[, column_name, ...])]
    * expressions:
    * : expression[, expression, ...]
    * sort_expressions:
    * : expression [ASC|DESC][, expression [ASC|DESC], ...]
    */

  /**
    * case...when...then Statements
    */

  val caseWhenThen= """SELECT
                      |CASE WHEN DEST_COUNTRY_NAME = 'UNITED STATES' THEN 1
                      |WHEN DEST_COUNTRY_NAME = 'Egypt' THEN 0
                      |ELSE -1 END
                      |FROM partitioned_flights"""
  spark.sql(caseWhenThen).show()
}
