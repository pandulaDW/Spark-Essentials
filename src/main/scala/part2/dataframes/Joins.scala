package part2.dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr}

object Joins extends App {

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json(path = "src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json(path = "src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json(path = "src/main/resources/data/bands.json")

    guitarsDF.show()
    guitaristsDF.show()
//    bandsDF.show()

  // inner joins
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF
    .join(bandsDF, joinExprs = joinCondition, joinType = "inner")

//  guitaristsBandsDF.show()

  // outer joins
  // left outer = everything in the in inner join + all the rows in the LEFT table
  guitaristsDF.join(bandsDF, joinCondition, joinType = "left_outer")

  // right outer = everything in the in inner join + all the rows in the RIGHT table
  guitaristsDF.join(bandsDF, joinCondition, joinType = "right_outer")

  // full outer = everything in the in inner join + all the rows in the BOTH tables
  guitaristsDF.join(bandsDF, joinCondition, joinType = "outer")

  // semi-join (like left-inner, but not including the right table)
  guitaristsDF.join(bandsDF, joinCondition, joinType = "left_semi")

  // anti-joins (non matching rows)
  guitaristsDF.join(bandsDF, joinCondition, joinType = "left_anti")

  // things to bear in mind
  // guitaristsBandsDF.select("id", "band").show() // this crashes

  // option 1 = rename the column on which we are joining
  guitaristsDF.join(bandsDF.withColumnRenamed(existingName = "id", newName = "band"), usingColumn = "band")

  // option 2 - drop the duplicate column
  guitaristsBandsDF.drop(bandsDF.col(colName = "id"))

  // option 3 - rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("bandId"))

// using complex types
  guitaristsDF
    .join(guitarsDF.withColumnRenamed(existingName = "id", newName = "guitarId"), expr(expr = "array_contains(guitars, guitarId)"))
    .show()
}
