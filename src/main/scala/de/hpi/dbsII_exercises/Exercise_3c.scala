package de.hpi.dbsII_exercises

import java.sql.Timestamp

import org.apache.spark.sql.{Dataset, SparkSession, Column}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
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
    //changeRecords.show()
    var result = changeRecords.groupBy($"tableId",$"attributeName",$"entityId",$"timestamp")
      .agg(collect_list($"newValue") as "newValues")
      .map(x => (x.getString(0), x.getString(1), x.getInt(2), x.getTimestamp(3), x.getSeq[String](4), x.getSeq[String](4).size))
      .filter(x => x._6 > 1)
      .map(x => ((x._1, x._2, x._3, x._4), x._5))
      .collect()

    result.toMap[(String, String, Int, Timestamp), Seq[String]]

      
    // var result = firstPart.map(row => Map((row.getString(0),row.getString(1),row.getInt(2),row.getTimestamp(3)) -> (row.getSeq[String](4)))
    //   .collect()
    //   .toMap
    //firstPart.show()
    //Map()
  }

  def collect_list(col: org.apache.spark.sql.Column): org.apache.spark.sql.Column = {
    org.apache.spark.sql.functions.collect_list(col)
  }

  // def countNumbers(col: org.apache.spark.sql.Column): org.apache.spark.sql.Column = {
  //   col.apply("count")

  //   }
    
  // }
}