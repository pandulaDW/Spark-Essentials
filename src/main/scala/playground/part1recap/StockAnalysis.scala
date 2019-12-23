package playground.part1recap

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, max}

object StockAnalysis extends App {

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val spark = SparkSession.builder()
    .appName("Stock Analysis")
    .config("spark.master", "local")
    .getOrCreate()

  val stocks = spark.read
    .format(source = "csv")
    .option("sep", ",")
    .option("header", "true")
    .load(path = "src/main/resources/data/aa.us.txt")

  // head method will return the first row
  // need to check the schema before calling get* methods
  val max_volume = stocks
    .agg(max(col("Volume")))
    .head()
    .getString(0)

  // Maximum volume traded
  stocks.select("Date", "Volume")
    .filter(col(colName = "Volume") === max_volume.toInt)
    .show()
}

// max(col(colName = "Volume")
