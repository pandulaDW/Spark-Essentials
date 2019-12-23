package part2.dataframes

import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.types.{LongType, StructField, StructType}

object DataFrameBasics extends App {
  // creating a SparkSession
  val spark = SparkSession.builder()
    .appName(name = "DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  // reading a DF
  val firstDF = spark.read
    .format(source = "json")
    .option("inferSchema", "true")
    .load(path = "src/main/resources/data/cars.json")

  // Showing a DF
  firstDF.show()
  firstDF.printSchema()

  // get rows
  firstDF.take(10).foreach(println)

  // spark types
  val longType = LongType

  // schema
  val carsSchema = StructType(
    Array(
      StructField("Name", types.StringType),
      StructField("Miles_per_Gallon", types.IntegerType),
      StructField("Cylinders", types.IntegerType),
      StructField("Displacement", types.IntegerType),
      StructField("Horsepower", types.IntegerType),
      StructField("Weight_in_lbs", types.IntegerType),
      StructField("Acceleration", types.DoubleType),
      StructField("Year", types.IntegerType),
      StructField("Origin", types.IntegerType)
    )
  )

  // obtain the inferred schema
  val carsDFSchema = firstDF.schema

  // read a DF with your schema
  val carsDFWithSchema = spark.read
    .format(source = "json")
    .schema(carsDFSchema)
    .load(path = "src/main/resources/data/cars.json")

  // create rows by hand
  val myRow = Row("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA")

  // create DF from tuples
  val cars = Seq(
    ("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"),
    ("buick skylark 320", 15.0, 8L, 350.0, 165L, 3693L, 11.5, "1970-01-01", "USA"),
    ("plymouth satellite", 18.0, 8L, 318.0, 150L, 3436L, 11.0, "1970-01-01", "USA"),
    ("ford torino", 17.0, 8L, 302.0, 140L, 3449L, 10.5, "1970-01-01", "USA"),
    ("ford galaxie 500", 15.0, 8L, 429.0, 198L, 4341L, 10.0, "1970-01-01", "USA"),
    ("chevrolet impala", 14.0, 8L, 454.0, 220L, 4354L, 9.0, "1970-01-01", "USA"),
    ("plymouth fury iii", 14.0, 8L, 440.0, 215L, 4312L, 8.5, "1970-01-01", "USA"),
    ("pontiac catalina", 14.0, 8L, 455.0, 225L, 4425L, 10.0, "1970-01-01", "USA"),
    ("amc ambassador dpl", 15.0, 8L, 390.0, 190L, 3850L, 8.5, "1970-01-01", "USA"),
    ("amc rebel sst", 16.0, 8L, 304.0, 150L, 3433L, 12.0, "1970-01-01", "USA")
  )

  val manualCarsDF = spark.createDataFrame(cars) // schema auto-inferred
  // note: DFs have schemas, rows do not

  // create DFs with implicits

  import spark.implicits._

  val manualCarsDFWithImplicits = cars.toDF(colNames = "Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "Origin")

  manualCarsDF.printSchema()
  manualCarsDFWithImplicits.printSchema()

  // Exercise:
  // 1) Create a manual DF describing smart phones
  val phoneSchema = StructType(
    Array(
      StructField("Name", types.StringType),
      StructField("Model", types.StringType),
      StructField("ScreenDimension", types.StringType),
      StructField("CameraPixels", types.DoubleType, nullable = true)
    )
  )

  val phones = Seq(
    Row("Nokia", "S7", "Android", 5.2),
    Row("Apple", "I7", "IOS", 8.0),
    Row("Samsung", "J7", "Windows", null),
  )

  val phonesDF = spark.createDataFrame(spark.sparkContext.parallelize(phones), phoneSchema)
  phonesDF.show()
  phonesDF.printSchema()

  // 2) Read another file (movies.json)
  val movies = spark.read
    .format(source = "json")
    .option("inferSchema", "true")
    .load(path = "src/main/resources/data/movies.json")

  movies.show()
  movies.printSchema()
  println(s"The movies DF has ${movies.count()} no of rows")
}
