package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DF columns and expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  carsDF.show()

  //  Columns
  val firstColumn = carsDF.col("Name")

  //  selecting (projection)
  val carNamesDF = carsDF.select(firstColumn)

  carNamesDF.show()

  //  various select methods

  import spark.implicits._

  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"), //  equivalent to carsDF.col("Acceleration"),
    column("Weight_in_lbs"), //equivalent to ^^
    //  Using implicits:
    'Year, // scala symbol, converted to column
    $"Horsepower", // fancier interpolated string, returns a Column object
    expr("Origin") //  Expression, returns the origin column
  )

  //  select with plain column names
  carsDF.select("Name", "Year")

  //  EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = simplestExpression / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2"),
  )

  carsWithWeightsDF.show()


  //
  val carsWithSelectExprWeightDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2",
  )

  carsWithSelectExprWeightDF.show()

  //  DF processing
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
  //  Renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight_in_pounds")
  //  careful with column names
  //  carsWithColumnRenamed.selectExpr("`Weight in pounds`")
  //  remove a column
  carsWithColumnRenamed.drop("Cylinders")
  //  filtering
  val nonUsaCars = carsDF.filter(col("Origin") =!= "USA")
  val nonUsaCars2 = carsDF.where(col("Origin") =!= "USA")
  //  filtering with expression strings
  val usaCarsDF = carsDF.filter("Origin = 'USA'")
  //  you can do chain filtering
  val usaPowerFulCarsDF = carsDF
    .filter(col("Origin") =!= "USA")
    .filter(col("Horsepower") > 50)
  val usaPowerFulCarsDF2 = carsDF
    .filter((col("Origin") =!= "USA") and (col("Horsepower") > 50))
  val usaPowerFulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  //  unioning (adding more rows)
  val moreCarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/more_cars.json")

  val allCarsDF = carsDF.union(moreCarsDF) //works if its the same schema
  val allCountriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show()
}
