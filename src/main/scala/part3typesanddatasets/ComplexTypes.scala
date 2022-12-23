package part3typesanddatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {

  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  //  Dates
  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
  val moviesWithReleaseDates = moviesDF
    .select(
      col("Title"),
      to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release")
    )

  moviesWithReleaseDates
    .withColumn("Today", current_date())
    .withColumn("Right_now", current_timestamp())
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365)
  //    .show()

  moviesWithReleaseDates
    .select("*")
    .where(col("Actual_Release").isNull)
    .show()
  //  some of the movies in this list do have a release date but it's in a different format

  /* EXERCISES
    1. How do we deal with multiple date formats
    2. Read stocks.csv and parse the dates
   */

  //  1 - Parse the DF multiple times, then union the small DFs
  //  This is part of the data cleaning process, there's no easy way around it
  //  We could ignore data if it's insignificant and we have a big volume of data

  //  2
  val stocksDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/stocks.csv")

  val stocksWithDatesDF = stocksDF
    .withColumn("actual_date", to_date(col("date"), "MMM dd yyyy"))
  //    .show()

  //  Structures (structs)
  //  1 - with col operators
  moviesDF
    .select(
      col("Title"),
      struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit")
    )
    .select(
      col("Title"),
      col("Profit").getField("US_Gross").as("US_Profit")
    )

  // with expression strings
  moviesDF
    .selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")

  //  Arrays
  val moviesWithWords = moviesDF
    .select(
      col("title"),
      split(col("Title"), " |,").as(("Title_Words"))
    )

  moviesWithWords
    .select(
      col("Title"),
      expr("Title_Words[0]"),
      size(col("Title_Words")),
      array_contains(col("Title_Words"), "Love")
    )
    .show()


}
