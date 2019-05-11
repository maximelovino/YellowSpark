import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import readers.{GeoReader, TaxiReader}


/**
  * @author Maxime Lovino
  * @date 2019-05-06
  * @package
  * @project YellowSpark
  */


object YellowSpark extends App {
  val spark = SparkSession.builder()
    .appName("Spark Taxi example")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._



  val boroughs = GeoReader.parseBoroughs()
  val finalDf = TaxiReader.parseTaxiData(spark, boroughs)

  finalDf.printSchema()
  finalDf.write.parquet("./src/main/resources/rides.df")

  val analytics = new TaxiAnalytics(spark)

  analytics.displayBoroughStats(finalDf)

  println(s"Number of rides total: ${finalDf.count()}")

  analytics.displayRateCodeStats(finalDf)

  analytics.displayTipStatsByBorough(finalDf)

  analytics.topDrivers(finalDf)

}
