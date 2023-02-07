package part5lowlevel

import org.apache.spark.sql.SparkSession

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
  case class StockValue(company: String, date: String, price: Double)

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
}
