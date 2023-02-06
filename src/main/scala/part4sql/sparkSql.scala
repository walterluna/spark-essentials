package part4sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object sparkSql extends App {
  val spark = SparkSession.builder()
    .appName("Spark SQL practice")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    //    .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  //  Using Spark DF API
  carsDF.select(col("Name")).where(col("Origin") === "USA")

  //  Using Spark SQL
  carsDF.createOrReplaceTempView("cars")
  val usCars = spark.sql(
    """
      |select Name from cars where Origin = 'USA';
      |""".stripMargin)
  //  usCars.show()

  //  we can run ANY SQL statement
  spark.sql("create database rtjvm;")
  spark.sql("use rtjvm;")
  val dbsDF = spark.sql("show databases")

  dbsDF.show()

  //  Transfer tables from a DB to spark tables
  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", s"public.$tableName")
    .load()


  def transferTables(tableNames: List[String]) = tableNames.foreach { tableName =>
    val tableDF = readTable(tableName)
    tableDF.createOrReplaceTempView(tableName)
    tableDF.write
      .mode(SaveMode.Overwrite)
      .saveAsTable(tableName)
  }

  transferTables(List(
    "employees",
    "departments",
    "titles",
    "dept_emp",
    "salaries",
    "dept_manager"
  ))

  //  read DF from warehouse
  val employeesDF2 = spark.read
    .table("employees")
}
