package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, count, countDistinct, mean, min, stddev, sum}

object Aggregations extends App {
  val spark = SparkSession.builder()
    .appName("Aggregations and grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  //  counting
  val genresCountDF = moviesDF.select(count(col("Major_Genre"))) // all values except null
  moviesDF.selectExpr("count(Major_Genre)") // equivalent ^^
  genresCountDF.show()

  //  counting all
  moviesDF.select(count("*")).show() // count all rows, including nulls

  //  counting distinct
  moviesDF.select(countDistinct("Major_Genre")).show()

  //  aproximate count
  moviesDF.select(approx_count_distinct(col("Major_Genre"))).show()

  //  min and max
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")

  //  sum
  moviesDF.select(sum(col("US_Gross")))
  moviesDF.selectExpr("sum(US_Gross)")

  //  avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  //  data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating")),
  ).show()

  //  grouping
  val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .count() //select count(*) from table group by Major_Genre
    .show()

  val avgRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")


  moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))
    .show()

  /* EXERCISES
  1. Sum all profits of all the movies
  2. count distinct directors
  3. show mean and stddev of US gross revenue
  4. compute the avg IMDB rating and avg US Gross
   */

  //  1. Sum all profits of all the movies
  moviesDF.selectExpr("sum(US_Gross + Worldwide_Gross)").show()

  //  2. count distinct directors
  moviesDF.select(countDistinct("Director")).show()

  //  3. show mean and stddev of US gross revenue
  moviesDF.select(
    mean(col("US_Gross")),
    stddev(col("US_Gross")),
  ).show()

  //  4. compute the avg IMDB rating and avg US Gross for each director
  moviesDF
    .groupBy("Director")
    .agg(
      avg(("IMDB_Rating")).as("Avg_IMDB_Rating"),
      avg("US_Gross").as("Avg_US_Gross"),
    )
    //    .orderBy(col("Avg_IMDB_Rating").desc)
    .orderBy(col("Avg_US_Gross").desc)
    .show()


}
