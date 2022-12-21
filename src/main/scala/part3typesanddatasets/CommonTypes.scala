package part3typesanddatasets

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{regexp_extract, _}

object CommonTypes extends App {

  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  //  Adding a plain value to a DF
  moviesDF.select(col("Title"), lit(47).as("plain_value"))
    .show()

  //  Booleans
  val dramaFilter = col("Major_Genre") === "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter

  moviesDF.select("Title").where(preferredFilter)

  val moviesWitGoodnessFlagsDF = moviesDF
    .select(col("Title"), preferredFilter.as("good_movie"))

  //  filter on boolean column
  moviesWitGoodnessFlagsDF
    .where("good_movie") // where(col("good_movie") === "true)

  //  negations
  moviesWitGoodnessFlagsDF
    .where(not(col("good_movie")))
    .show()

  //  Numbers
  //  math operators
  val moviesAvgRatingsDF = moviesDF
    .select(
      col("Title"),
      (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating") / 2).as("Avg_Rating")
    )
    .show()

  //  correlation = number between -1 and 1
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating"))

  //  Strings
  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  //  capitalization: initcap, lower, upper
  carsDF.select(
    initcap(col("Name"))
  )

  //  contains
  carsDF.
    select("*")
    .where(col("Name").contains("volkswagen"))

  //  regex
  val regexString = "volkswagen|vw"
  val vwDF = carsDF
    .select(
      col("Name"),
      regexp_extract(col("Name"), regexString, 0).as("regex_extract")
    )
    .where(col("regex_extract") =!= "")
    .drop("regex_extract")

  vwDF
    .select(
      col("Name"),
      regexp_replace(col("Name"), regexString, "People's car")
    )
    .show

  /* EXERCISE
  Filter cars by a list of car names obtained by an API call
   */

  def getCarsByNames(names: List[String]): DataFrame = {
    val namesRegex = names.mkString("|")
    carsDF
      .select(
        col("Name"),
        regexp_extract(col("Name"), namesRegex, 0).as("regex_result")
      )
      .where(col("regex_result") =!= "")
      .drop("regex_result")
  }

  getCarsByNames(List("vw")).show()


}
