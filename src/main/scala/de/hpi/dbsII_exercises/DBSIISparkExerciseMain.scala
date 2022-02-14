package de.hpi.dbsII_exercises

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object DBSIISparkExerciseMain extends App{

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

  val pathToData = "/Users/Johann/Documents/GitHub/spark-tutorial/Test_Input_Spark"//args(0)
  val numCores = 4//args(1).toInt
  val numShufflePartitions = 8
  //println(pathToData)
  val sparkBuilder = SparkSession
    .builder()
    .appName("SparkTutorial")
    .master(s"local[$numCores]") // local, with 4 worker cores
  val spark = sparkBuilder.getOrCreate()
  import spark.implicits._

  // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
  spark.conf.set("spark.sql.shuffle.partitions", 8) //

  val changeRecords = IOHelper.readSparkCSV(spark,pathToData,true)
    .map(r => ChangeRecord.from(r))
    .cache()
  // println(s"Number of records: ${changeRecords.count()}")
  val res3a = new Exercise_3a(spark,changeRecords).execute()
  val res3b = new Exercise_3b(spark,changeRecords).execute()
  val res3c = new Exercise_3c(spark,changeRecords).execute()
  val res3d = new Exercise_3d(spark,changeRecords).execute()

  val resultChecker = new ResultChecker(spark)
  resultChecker.checkExercise1Result(res3a,"data/exercise1.csv")
  //println(res3b)
  resultChecker.checkExercise2Result(res3b,"data/exercise2.csv")
  //println(res3c)
  resultChecker.checkExercise3Result(res3c,"data/exercise3.json")
  resultChecker.checkExercise4Result(res3d,"data/exercise4.json")

}
