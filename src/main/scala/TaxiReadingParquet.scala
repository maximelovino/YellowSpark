import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import readers.{GeoReader, TaxiReader}


/**
  * @author Maxime Lovino
  * @date 2019-05-06
  * @package
  * @project YellowSpark
  */


object TaxiReadingParquet extends App {
  val spark = SparkSession.builder()
    .appName("Spark Taxi example")
    .master("local[*]")
    .getOrCreate()

  val boroughs = GeoReader.parseBoroughs()
  val finalDf = TaxiReader.parseTaxiData(spark, boroughs)

  finalDf.printSchema()
  finalDf.write.parquet("./src/main/resources/rides.df")
}
