package part3typesanddatasets

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Date

object DataSets extends App {
  val spark = SparkSession.builder()
    .appName("DataSets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()

  //  Convert a DataFrame to a DataSet
  implicit val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]
  numbersDS.filter(_ > 100)

  //  DataSet of a complex type
  //  1 - define your case class
  case class Car(
                  Name: String,
                  Miles_per_gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: Date,
                  Origin: String
                )

  //  2 - read the DF from the file
  def readDF(filename: String) =
    spark.read
      .option("inferSchema", "true")
      .json(s"src/main/resources/data/$filename")

  //  3 - define an encoder

  import spark.implicits._
  //  implicit val carEncoder = Encoders.product[Car]

  //  val carsDF = readDF("cars.json") // there's a bug with this version of spark, I had to create the DF with an explicit schema
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))
  val carsDF = spark.read
    .schema(carsSchema)
    .json("src/main/resources/data/cars.json")
  //  4- convert the DF to DS
  val carsDS = carsDF.as[Car]

  //  DS collection functions
  numbersDS.filter(_ < 100)

  //  access to map. flatMap, fold, reduce, for comprehensions
  val carNamesDS = carsDS.map(_.Name.toUpperCase())


  /* EXERCISES
  1. Count how many cars we have
  2. Count how many powerful cars we have (HP>140)
  3. Average HP for the entire DS
   */

  //  1
  val numberOfCars = carsDS.count()
  println(numberOfCars)

  //  2
  val powerfulCars = carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count()
  println(powerfulCars)

  //  3
  val avgHorsepower = carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / numberOfCars
  println(avgHorsepower)
  // also use the DF functions!
  carsDS.select(avg(col("Horsepower")))
}
