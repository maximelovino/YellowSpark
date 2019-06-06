import org.apache.spark.sql.SparkSession
import readers.{GeoReader, TaxiReader}


/**
  * @author Maxime Lovino
  * @date 2019-05-06
  * @package
  * @project YellowSpark
  */


object TaxiReadingParquet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Taxi example")
      .getOrCreate()

    val boroughs = GeoReader.parseBoroughs()
    val finalDf = TaxiReader.parseTaxiData(spark, boroughs)

    finalDf.printSchema()
    finalDf.write.parquet("s3a://yellowspark-us-new/rides_final.df")
  }
}
