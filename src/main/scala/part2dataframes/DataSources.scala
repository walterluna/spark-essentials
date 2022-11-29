package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object DataSources extends App {

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

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

  /**
    * Reading a DF
    * -format
    * -path
    * -schema (optional if schema is inferred)
    * -zero or more options
    */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema) //enforce a schema
    .option("mode", "failFast") //other options: dropMalformed, permissive(default)
    .option("path", "src/main/resources/data/cars.json")
    .load()

  //  alternative with options map
  val carsDFWithOptions = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true",
    ))

  /**
    * writing DF
    * -format
    * -save mode = overwrite, append, ignore, errorIfExists
    * -path
    * -zero or more options
    */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_dup.json")


  //  JSON flags
  spark.read
    //  .format("json")
    .schema(carsSchema)
    .option("dateFormat", "yyyy-MM-dd") // must specify  schema. If spark fails parsing it will be null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json") // load() with json format

  //  CSV flags
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType),
  ))

  spark.read
    //    .format("csv")
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd yyyy") // must specify  schema. If spark fails parsing it will be null
    .option("header", "true") // first row is header
    .option("sep", ",") // separator
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv") // load() with csv format

  //  Parquet (default storage format for DF)
  carsDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/cars.parquet") // save will do the same without any format()

  //  Text file
  spark.read
    .text("src/main/resources/data/sampleTextFile.txt")
    .show()

  //  Reading from a remote DB
  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  employeesDF.show()

  /**
    * EXERCISES
    * 1) read the movies DF and write it as:
    * -tab-separated values file
    * -snappy parquet
    * -table in the pg db "public.movies"
    */

  val exMoviesDF = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/movies.json")

  exMoviesDF.write
    .mode(SaveMode.Overwrite)
    .format("csv")
    .option("sep", "\t")
    .option("header", "true")
    .save("src/main/resources/data/movies.csv")

  exMoviesDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/movies.parquet")

  exMoviesDF.write
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.movies")
    .save()

}
