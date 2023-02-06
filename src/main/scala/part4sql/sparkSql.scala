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


  def transferTables(tableNames: List[String], shouldWrite: Boolean = false) = tableNames.foreach { tableName =>
    val tableDF = readTable(tableName)
    tableDF.createOrReplaceTempView(tableName)
    if (shouldWrite) {
      tableDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }
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

  /** Exercises
    * 1. Read the movies DF and store it as a Spark table in rtjvm table
    * 2. Count how many employees were hired in between Jan 1 1999 and Jan 1 2000
    * 3. Show the avg salaries for the employees hired in those days, grouped by dept_no
    * 4. show the name of the best paying department for employees hired in between those dates
    */

  //  Ex1
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")
  moviesDF.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("movies")

  //  Ex2
  val ex2 = spark.sql(
    """
      |SELECT
      |   count(*)
      |FROM employees e
      |   WHERE e.hire_date BETWEEN '1999-01-01' AND '2000-01-01'
    """.stripMargin)
  println("employees hired between: ", ex2)

  //  Ex3
  val ex3 = spark.sql(
    """
      |SELECT
      |   de.dept_no,
      |   avg(s.salary) avg_sal
      |FROM
      |   employees e
      |   LEFT JOIN salaries s
      |       ON e.emp_no = s.emp_no
      |   LEFT JOIN dept_emp de
      |       ON e.emp_no = de.emp_no
      |GROUP BY
      |   de.dept_no
      |ORDER BY
      |   avg_sal DESC;
    """.stripMargin)

  println("Average salaries for those employees grouped by department")
  ex3.show()

  //  Ex4
  val ex4 = spark.sql(
    """
      |SELECT
      |   d.dept_name,
      |   avg(s.salary) avg_sal
      |FROM
      |   employees e
      |   LEFT JOIN salaries s
      |       ON e.emp_no = s.emp_no
      |   LEFT JOIN dept_emp de
      |       ON e.emp_no = de.emp_no
      |   LEFT JOIN departments d
      |       ON d.dept_no = de.dept_no
      |GROUP BY
      |   d.dept_name
      |ORDER BY
      |   avg_sal DESC
      |LIMIT 1;
      """.stripMargin)

  println("Top earning department is:")
  ex4.show()
}
