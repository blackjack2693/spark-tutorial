package de.hpi.dbsII_exercises

import org.apache.spark.sql.{Dataset, SparkSession}

/***
 * Geben Sie für jede Tabelle die Anzahl der Attribute zurück.
 * @param spark - the spark context
 * @param changeRecords - the input spark dataset containing the change records
 */
class Exercise_3b(spark: SparkSession, changeRecords: Dataset[ChangeRecord]) {

  import spark.implicits._

  /***
   * |  tableID|          timestamp|entityID|       attributeName|            newValue|

   * @return A Map that maps a table-id (key) to the number of attributes of the corresponding table (value)
   */
  def execute():Map[String,Int] = {
    //println(changeRecords.show())
    var resultMap =changeRecords
      .select("tableId","AttributeName")
      .distinct()
      .groupBy("tableId")
      .count()// returns (tableId,count)
      .map(x => (x.getString(0),x.getLong(1).toInt))
      .collect()
      .toMap
      // println(resultMap)
    resultMap
  }

}
