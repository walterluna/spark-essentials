package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object DataFramesBasics extends App {
  // creating a SparkSession
  val spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  // reading a DF
  val firstDF = spark.read
    .format("json")
    // in prod, we would not infer a schema
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  // showing a DF
  firstDF.show()
  firstDF.printSchema()

  //  get rows from a DF
  firstDF.take(10).foreach(println)

  //  spark types
  val longType = LongType

  //  schema
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  // obtain a schema
  val carsDfSchema = firstDF.schema
  println(carsDfSchema)

  //  read a DF with a given schema
  var carsDFWithSchema = spark.read
    .format("json")
    .schema(carsDfSchema)
    .load("src/main/resources/data/cars.json")


  //  create rows by hand
  val myRow = Row(
    ("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"),
  )

  //  Create DF from tuples
  val cars = Seq(
    ("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"),
    ("buick skylark 320", 15.0, 8L, 350.0, 165L, 3693L, 11.5, "1970-01-01", "USA"),
    ("plymouth satellite", 18.0, 8L, 318.0, 150L, 3436L, 11.0, "1970-01-01", "USA"),
    ("amc rebel sst", 16.0, 8L, 304.0, 150L, 3433L, 12.0, "1970-01-01", "USA"),
    ("ford torino", 17.0, 8L, 302.0, 140L, 3449L, 10.5, "1970-01-01", "USA"),
    ("ford galaxie 500", 15.0, 8L, 429.0, 198L, 4341L, 10.0, "1970-01-01", "USA"),
    ("chevrolet impala", 14.0, 8L, 454.0, 220L, 4354L, 9.0, "1970-01-01", "USA"),
    ("plymouth fury iii", 14.0, 8L, 440.0, 215L, 4312L, 8.5, "1970-01-01", "USA"),
    ("pontiac catalina", 14.0, 8L, 455.0, 225L, 4425L, 10.0, "1970-01-01", "USA"),
    ("amc ambassador dpl", 15.0, 8L, 390.0, 190L, 3850L, 8.5, "1970-01-01", "USA"),
  )
  val manualCarsDf = spark.createDataFrame(cars) // schema auto-inferred

  //  note: DFs have schemas, rows do not

  //  create DFs with implicits

  import spark.implicits._

  val manualCarsDFWithImplicits = cars
    .toDF(" Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "CountryOrigin")

  manualCarsDf.printSchema()
  manualCarsDFWithImplicits.printSchema()

  /** EXERCISE
    * 1) Create a manual DF describing smartphones
    * -make
    * -model
    * -screen dimensions
    * -camera mp
    * 2) Read another file from data/ e.g. movies.json
    * -printSchema
    * -count rows, count()
    */

  val phonesSchema = StructType(Array(
    StructField("make", StringType),
    StructField("model", StringType),
    StructField("screenDimensions", StringType),
    StructField("cameraMP", IntegerType),
  ))

  val phones = Seq(
    ("Apple", "iPhone 14", "6.5 in", 12),
    ("Samsung", "S 22", "6.6 in", 18),
    ("Samsung", "Fold 2", "7.0 in", 20),
  )

  //  val phonesDF = spark.createDataFrame(phones)
  val phonesDF = phones.toDF("make", "model", "screenDimensions", "cameraMP")
  phonesDF.show()
  phonesDF.printSchema()

  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  moviesDF.printSchema()
  println(s"There are: ${moviesDF.count()} movies in the DF")


}
