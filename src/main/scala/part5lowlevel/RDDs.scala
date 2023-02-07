package part5lowlevel

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import scala.io.Source

object RDDs extends App {
  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext

  //  Ways to create an RDD
  //  1- parallelize an existing collection
  val numbers = 1 to 100000
  val numbersRDD = sc.parallelize(numbers)

  //  2 reading from files
  case class StockValue(symbol: String, date: String, price: Double)

  def readStocks(fileName: String) =
    Source.fromFile(fileName)
      .getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

  val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  //  2b - reading from files
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase() == tokens(0))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  //  read from a DF (easiest)
  val stocksDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  import spark.implicits._

  val stocksDS = stocksDF.as[StockValue]
  val stocksRDD3 = stocksDS.rdd


  //  Convert from RDD to DF
  val numbersDF = numbersRDD.toDF("numbers") // you lose type information
  //  Convert from RDD to DS
  val numbersDS = spark.createDataset(numbersRDD) // you keep the type information

  //  TRANSFORMATIONS
  val msftRDD = stocksRDD.filter(_.symbol == "MSFT") // lazy transformation
  val msCount = msftRDD.count() //eager ACTION

  val companyNamesRDD = stocksRDD.map(_.symbol)
    .distinct() // distinct is a lazy transformation

  //  min and max
  implicit val stockOrdering = Ordering.fromLessThan[StockValue]((sa, sb) => sa.price < sb.price)
  val minMsft = msftRDD.min() // also an action

  //  reduce
  numbersRDD.reduce(_ + _) // lazy

  //  grouping
  val groupedStocksRDD = stocksRDD.groupBy(_.symbol)
  // grouping is very expensive, it does shuffling

  //  PARTITIONING
  val repartitionedStocksRdd = stocksRDD.repartition(30)
  repartitionedStocksRdd.toDF().write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks30")
  /*
    Repartition is expensive as it involves shuffling
    Best practice: partition EARLY, then process that.
    Size of a partition 10-100MB.
   */

  //  coalesce
  val coalesceRDD = repartitionedStocksRdd.coalesce(15) // does NOT involve shuffling


  /** EXERCISES
    * 1. Read movies.json as an RDD
    * 2. Show the distinct genres as and RDD
    * 3. Select all the movies in the drama genre with IMDB rating > 6
    * 4. Show the avg rating of movies by genre
    */

  case class Movie(title: String, genre: String, rating: Double)

  //  Ex1
  val moviesDF = spark.read
    .option("inferSchemea", "true")
    .json("src/main/resources/data/movies.json")

  val moviesRDD = moviesDF
    .select(col("Title").as("title"), col("Major_Genre").as("genre"), col("IMDB_Rating").as("rating"))
    .where(col("IMDB_Rating").isNotNull and col("Major_Genre").isNotNull)
    .as[Movie]
    .rdd

  //  Ex2
  val distinctGenresRDD = moviesRDD.map(_.genre).distinct()

  //  Ex3
  val goodDramasRDD = moviesRDD.filter(movie => movie.genre == "Drama" && movie.rating > 6)

  //  Ex4
  case class GenreAvgRating(genre: String, average: Double)

  val avgRatingByGenre = moviesRDD
    .groupBy(_.genre)
    .map {
      case (genre, movies) => GenreAvgRating(genre, movies.map(_.rating).sum / movies.size)
    }

  moviesRDD.toDF().show()
  distinctGenresRDD.toDF().show()
  goodDramasRDD.toDF().show()
  avgRatingByGenre.toDF().show()
}
