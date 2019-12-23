package part2.dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession, types}
import org.apache.spark.sql.types.{StructField, StructType}

object DataSources extends App {

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val spark = SparkSession.builder()
    .appName(name = "Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(
    Array(
      StructField("Name", types.StringType),
      StructField("Miles_per_Gallon", types.IntegerType),
      StructField("Cylinders", types.IntegerType),
      StructField("Displacement", types.IntegerType),
      StructField("Horsepower", types.IntegerType),
      StructField("Weight_in_lbs", types.IntegerType),
      StructField("Acceleration", types.DoubleType),
      StructField("Year", types.DateType),
      StructField("Origin", types.IntegerType)
    )
  )
  /*
   Reading a DF:
    - format
    - schema or inferSchema = true
    - path
    - zero or more options
   */
  val carsDF = spark.read
    .format(source = "json")
    .schema(carsSchema) // enforce a schema
    .option("mode", "permissive") // dropMalformed, permissive (default)
    .option("path", "src/main/resources/data/cars.json") // another way
    .load()

  //  carsDF.show()

  // alternative reading with options map
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()

  //  carsDFWithOptionMap.show()

  // Need to persist in memory before writing to the file in windows
  carsDF.cache()

  /*
  Writing DFs
    - format
    - save mode = overwrite, append, ignore, errorIfExists
    - path
    - zero or more options
   */
  // Writing DFs
  carsDF.write
    .format("json")
    .mode(saveMode = SaveMode.Overwrite)
    .save(path = "src/main/resources/data/cars_dupe.json")

  // JSON flags
  spark.read
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd") // couple with schema, if Spark fails parsing, it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate (spark will decompress automatically)
    .json(path = "src/main/resources/data/cars_dupe.json")

  // CSV Flags
  // col names should match in the schema
  val stockSchema = StructType((
    Array(
      StructField("symbol", types.StringType),
      StructField("date", types.DateType),
      StructField("price", types.DoubleType)
    )
    ))

  spark.read
    .format(source = "csv")
    .schema(stockSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true")
    .option("sep", ",") // default is comma
    .option("nullValue", "") // parse this value as null
    .load(path = "src/main/resources/data/stocks.csv")

  // Parquet (open source compressed binary data storage optimized for fast column reading)
  // default in spark for writing DFs
  carsDF.write
    .mode(saveMode = SaveMode.Overwrite)
    .save(path = "src/main/resources/data/cars.parquet")

  // Text files
  spark.read.text(path = "src/main/resources/data/sampleTextFile.txt").show()

  // Reading from a remote DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.employees")
    .load()

  // Read the movies DF, then write it as
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json(path = "src/main/resources/data/movies.json")

  moviesDF.show()
  moviesDF.persist()

  // Tab separated csv
  moviesDF.write
    .format(source = "csv")
    .mode(saveMode = SaveMode.Overwrite)
    .option("sep", "\t")
    .option("header", "true")
    .save(path = "src/main/resources/data/movies.csv")

  // Snappy Parquet
  moviesDF.write
    .mode(saveMode = SaveMode.Overwrite)
    .save(path = "src/main/resources/data/movies.parquet")

}




