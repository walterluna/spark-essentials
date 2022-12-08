package part2dataframes

import org.apache.spark.sql.SparkSession

object Joins extends App {
  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val guitars = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  //  joins
  //  inner joins
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF.join(
    bandsDF,
    joinCondition,
    "inner" // default value
  )
  guitaristsBandsDF.show()

  //  -- outer joins --
  //  left outer
  guitaristsDF.join(
    bandsDF,
    joinCondition,
    "left_outer"
  )

  //  right outer
  guitaristsDF.join(
    bandsDF,
    joinCondition,
    "right_outer"
  )

  //  full outer join
  guitaristsDF.join(
    bandsDF,
    joinCondition,
    "outer"
  )

  //  semi-joins select only data on A, dont select from B
  guitaristsDF.join(
    bandsDF,
    joinCondition,
    "left_semi"
  )

  //  anti-joins select where condition is NOT true
  guitaristsDF.join(
    bandsDF,
    joinCondition,
    "left_anti"
  ).show()

}
