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


  private def displayBoroughStats(df: DataFrame): Unit = {
    df.groupBy($"pickup_borough").count().show()
    df.groupBy($"dropoff_borough").count().show()
  }

  private def displayRateCodeStats(df: DataFrame): Unit = {
    df.groupBy($"rate_code").count().show()
  }

  private def displayTipStatsByBorough(df: DataFrame): Unit = {
    df.groupBy($"pickup_borough").avg("tip_amount").show()
    df.groupBy($"dropoff_borough").avg("tip_amount").show()
  }


  private def topDrivers(df: DataFrame, count: Int = 20) = {
    val groupByLicense = df.groupBy("hack_license").count()
    val avgByLicense = df.groupBy("hack_license").avg("average_speed_kmh")
      .withColumnRenamed("avg(average_speed_kmh)", "Average speed in km/h")


    val sumByLicense = df.groupBy("hack_license")
      .sum("passenger_count", "trip_distance_km", "taxi_revenue", "tip_amount", "fare_amount", "trip_time_in_secs")
      .withColumnRenamed("sum(passenger_count)", "total_passengers")
      .withColumnRenamed("sum(trip_distance_km)", "total_distance_km")
      .withColumnRenamed("sum(taxi_revenue)", "$$$$$")
      .withColumnRenamed("sum(tip_amount)", "TIPS")
      .withColumnRenamed("sum(fare_amount)", "FARES")
      .withColumnRenamed("sum(trip_time_in_secs)", "DURATION")

    val aggByLicense = groupByLicense
      .join(sumByLicense, "hack_license")
      .join(avgByLicense, "hack_license")
      .orderBy(desc("$$$$$"))

    aggByLicense.show(count)
  }

  val boroughs = GeoReader.parseBoroughs()
  val finalDf = TaxiReader.parseTaxiData(spark, boroughs)

  finalDf.printSchema()
  finalDf.show(2)

  displayBoroughStats(finalDf)

  println(s"Number of rides total: ${finalDf.count()}")

  displayRateCodeStats(finalDf)

  displayTipStatsByBorough(finalDf)

  topDrivers(finalDf)

}
