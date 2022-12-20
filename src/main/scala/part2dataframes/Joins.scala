package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, max}

object Joins extends App {
  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val guitars = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  //  joins
  //  inner joins
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF.join(
    bandsDF,
    joinCondition,
    "inner" // default value
  )
  guitaristsBandsDF.show()

  //  -- outer joins --
  //  left outer
  guitaristsDF.join(
    bandsDF,
    joinCondition,
    "left_outer"
  )

  //  right outer
  guitaristsDF.join(
    bandsDF,
    joinCondition,
    "right_outer"
  )

  //  full outer join
  guitaristsDF.join(
    bandsDF,
    joinCondition,
    "outer"
  )

  //  semi-joins select only data on A, dont select from B
  guitaristsDF.join(
    bandsDF,
    joinCondition,
    "left_semi"
  )

  //  anti-joins select where condition is NOT true
  guitaristsDF.join(
    bandsDF,
    joinCondition,
    "left_anti"
  ).show()

  /* EXERCISES
  1. Show all employees and their max salaries
  2. Show all employees who were never managers
  3. Find the job titles of the best paid 10 employees
   */
  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", s"public.$tableName")
    .load()

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val deptManagersDF = readTable("dept_manager")
  val titlesDF = readTable("titles")

  //  1
  val maxSalariesPerEmployeeDF = salariesDF
    .groupBy("emp_no")
    .agg(max("salary").as("max_salary"))
  val employeesSalariesDF = employeesDF
    .join(maxSalariesPerEmployeeDF, "emp_no")
  employeesSalariesDF.show()

  //  2 (anti join)
  val empNeverManagersDF = employeesDF.join(
    deptManagersDF,
    employeesDF.col("emp_no") === deptManagersDF.col("emp_no"),
    "left_anti"
  )
  empNeverManagersDF.show()

  //  3
  val mostRecentJobTitlesDF = titlesDF
    .groupBy("emp_no", "title")
    .agg(max("to_date"))
  val bestPaidEmployeesDF = employeesSalariesDF
    .orderBy(col("max_salary").desc)
    .limit(10)
  val bestPaidJobsDF = bestPaidEmployeesDF
    .join(mostRecentJobTitlesDF, "emp_no")
  bestPaidJobsDF.show()


}
