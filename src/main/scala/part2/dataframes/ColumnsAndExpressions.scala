package part2.dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {

  // Remove info logs
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json(path = "src/main/resources/data/cars.json")

  // carsDF.show()

  // Columns
  val firstColumn = carsDF.col(colName = "Name")

  // selecting
  val carsNameDF = carsDF.select(firstColumn) // obtain a new DF since DFs are immutable

  // various select methods

  import spark.implicits._

  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala symbol, auto-converted to column
    $"HorsePower", // fancier interpolated string, returns a Column object
    expr("Origin") // Expression
  )

  // select with plain col names
  carsDF.select("Name", "Year")

  // EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")

  // this will return a column object
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeight = carsDF.select(
    col("Name"),
    col(colName = "Weight_in_lbs"),
    weightInKgExpression.as(alias = "Weight_in_kg"),
    expr(expr = "Weight_in_lbs / 2.2").as(alias = "Weight_in_kg_2")
  )

  // selectExpr
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // DF processing
  val carsWithKg3DF = carsDF.withColumn("Weight_in_lbs", col("Weight_in_lbs") / 2.2)

  // renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed(existingName = "Weight_in_lbs", newName = "Weight in pounds")

  // careful with column name, use backtick to make it a one expression
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")

  // remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // filtering (Not equal in spark is =!=)
  val europeanCarsDF = carsDF.filter(col(colName = "Origin") =!= "USA")
  val USACarsDF = carsDF.where(col(colName = "Origin") === "USA")

  // filtering with expression strings
  val americanCarsDF = carsDF.filter(conditionExpr = "Origin = 'USA'")

  // chain filters
  val americanPowerfulCars = carsDF
    .filter(col("Origin") === "USA")
    .filter(col("Horsepower") > 150)

  val americanPowerfulCars2 = carsDF
    .filter(col("Origin") === "USA" and col("Horsepower") > 150)

  val americanPowerfulCars3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")
  // println(americanPowerfulCars.count() == americanPowerfulCars3.count())

  // row concatenation = adding more rows
  val moreCarsDF = spark.read
    .option("inferSchema", "true")
    .json(path = "src/main/resources/data/more_cars.json")

  val allCarsDF = carsDF.union(moreCarsDF) // works if the DFs have the same schema

  // distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()
  // allCountriesDF.show()

  /*
  Exercises
    1) Read the movies DF and select 2 columns of your choice
    2) Create another column to sum up 3 profits
    3) Select all the GOOD comedy movies with IMDB above 6
   */
  val movies = spark.read
    .option("inferSchema", "true")
    .json(path = "src/main/resources/data/movies.json")

  movies.printSchema()

  val subDF = movies.select(
    col("Title"),
    col("Release_Date")
  )
  subDF.show()

  val totalIncome = movies.withColumn(colName = "Total Income", col = expr(expr = "US_Gross + Worldwide_Gross"))
  val totalIncome2 = movies.select(
    col("US_DVD_Sales"),
    col("US_Gross"),
    col("Worldwide_Gross"),
    (col("US_Gross") + col("Worldwide_Gross")).as("totalIncome")
  )

  val totalIncome3 = movies.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_Gross + Worldwide_Gross as Total_Gross"
  )

  totalIncome3.show()
  // totalIncome.select("`Total Income`").show()

  val goodComedyDF = movies
    .select("Major_Genre", "IMDB_Rating")
    .filter(col(colName = "Major_Genre") === "Comedy")
    .where(col(colName = "IMDB_Rating") > 6.0)

  val goodComedyDF2 = movies.select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)

  goodComedyDF2.show()

  println(movies.count())
  println(goodComedyDF.count())
}
