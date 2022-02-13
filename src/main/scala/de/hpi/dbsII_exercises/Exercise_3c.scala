package de.hpi.dbsII_exercises

import java.sql.Timestamp

import org.apache.spark.sql.{Dataset, SparkSession}

/***
 * Machen Sie einen Datenqualitätscheck: Wir würden annehmen, dass es in jeder Tabelle zujedem Zeitpunkt für jedes Feld
 * (identifiziert durch die Kombination aus entityID und attributeName) nur einen einzigen Wert geben kann.
 * Geben Sie für alle Felder aller Tabellen, bei denen dies nicht so ist für jeden Zeitpunkt alle Werte aus, die zu diesem Zeitpunkt vorkommen.
 * @param spark - the spark context
 * @param changeRecords - the input spark dataset containing the change records
 */
class Exercise_3c(spark: SparkSession, changeRecords: Dataset[ChangeRecord]) {

  import spark.implicits._

  //Ausgabenformat: (tableID,attribute,entity,timestamp) -> [Werte zu diesem Zeitpunkt]
  /***
   *
   * @return A map that contains all fields at all points in time, where there are multiple different values recorded for that field. Format:
   *         Key: (tableID,attribute,entity,timestamp) Value: a sequence of all values of the field tableId.attribute[entity] at timestamp t
   *         A field,timestamp-combination shall only appear in this map if there are multiple values for it at that point in time.
   *         That means for all values v in the returned map, the following must hold: v.size > 1.
   */
  def execute():Map[(String,String,Int,Timestamp),Seq[String]] = {
    changeRecords.show()
    var firstPart = changeRecords.groupBy($"tableId",$"attributeName",$"entityId",$"timestamp")
      .agg(collect_list($"newValue") as "newValues")
      .filter($"newValues.size" > 1)
      .collect()
    firstPart.foreach(println)
    // var result = firstPart.map(row => Map((row.getString(0),row.getString(1),row.getInt(2),row.getTimestamp(3)) -> (row.getSeq[String](4)))
    //   .collect()
    //   .toMap
    Map()
  }

  def collect_list(col: org.apache.spark.sql.Column): org.apache.spark.sql.Column = {
    org.apache.spark.sql.functions.collect_list(col)
  }

}