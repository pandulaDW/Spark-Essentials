package part2.dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{
  approx_count_distinct, avg, col, count, countDistinct,
  desc, expr, mean, min, stddev, sum
}

object Aggregations extends App {

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val spark = SparkSession.builder()
    .appName("Aggregation and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json(path = "src/main/resources/data/movies.json")

  // counting
  // distinct count
  val genresCount = moviesDF
    .select(col(colName = "Major_Genre"))
    .distinct()
    .count()

  // count of all the values except null
  val genresCount2 = moviesDF.select(count(col(colName = "Major_Genre")))
  genresCount2

  // count all the rows, and will INCLUDE nulls
  moviesDF.select(count(columnName = "*"))

  // counting distinct
  moviesDF.select(countDistinct(col(colName = "Major_Genre")))

  // for very large data sets
  moviesDF.select(approx_count_distinct(columnName = "Major_Genre"))

  // min and max
  val minRatingDF = moviesDF.select(min(col(colName = "IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")

  // sum
  moviesDF.select(sum(col(colName = "US_GROSS")))

  // avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))

  // data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  )

  // Grouping
  val countByGenreDF = moviesDF.groupBy(col("Major_Genre"))
    .count()

  val avgRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count(columnName = "*").as(alias = "allMovies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))

  // Total of the profits
  moviesDF
    .select((col("US_Gross") + col("Worldwide_Gross")).as("Total"))
    .select(sum("Total"))
    .show

  // Sum of all the profits in the movies
  val sumOfMoviesDF = moviesDF.groupBy(col("Title"))
    .agg(
      sum(columnName = "US_Gross").as(alias = "Total Gross"),
      sum(columnName = "Worldwide_Gross") as ("Worldwide Gross"),
      sum(expr(expr = "US_Gross + Worldwide_Gross")).as(alias = "Total Gross")
    )

  // Check distinct titles
  // println(moviesDF.count(), moviesDF.select(col(colName = "Title")).distinct().count())

  // Check distinct directors
  val directorsDF = moviesDF.groupBy(col("Director"))
    .agg(
      countDistinct("Title").as(alias = "#Movies"),
      avg(expr(expr = "US_Gross + Worldwide_Gross")).as(alias = "Total Gross"),
      avg("IMDB_Rating").as(alias = "Avg IMDB Rating"),
      avg("Rotten_Tomatoes_Rating").as(alias = "Avg Rotten Score")
    )
    .orderBy(col("`#Movies`").desc_nulls_last)

  directorsDF.show(numRows = 30)

  // check mean and stdev of us gross for movies
  val USGrossDF = moviesDF.groupBy(col("Title"))
    .agg(
      avg("US_Gross").as(alias = "Avg_Gross"),
      stddev("US_Gross").as(alias = "STDDev_Gross")
    )
  USGrossDF.show()
}
