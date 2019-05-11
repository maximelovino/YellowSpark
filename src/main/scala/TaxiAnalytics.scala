import java.sql.Timestamp

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * @author Maxime Lovino
  * @date 2019-05-11
  * @package
  * @project YellowSpark
  */

class TaxiAnalytics(spark: SparkSession) {

  import spark.implicits._


  def displayBoroughStats(df: DataFrame): Unit = {
    df.groupBy($"pickup_borough").count().show()
    df.groupBy($"dropoff_borough").count().show()
  }

  def statsByRateCode(df: DataFrame): DataFrame = {
    df.groupBy($"rate_code").count()
  }

  def statsByBoroughPairs(df: DataFrame): DataFrame = {
    val avg = df.groupBy($"pickup_borough", $"dropoff_borough").avg("tip_amount", "trip_time_in_secs", "trip_distance_km", "taxi_revenue")
    val sum = df.groupBy($"pickup_borough", $"dropoff_borough").sum("trip_time_in_secs", "trip_distance_km", "taxi_revenue")
    val count = df.groupBy($"pickup_borough", $"dropoff_borough").count()
    avg.join(count, Seq("pickup_borough", "dropoff_borough")).join(sum, Seq("pickup_borough", "dropoff_borough"))
      .orderBy("pickup_borough", "dropoff_borough")
  }


  def topDrivers(df: DataFrame, count: Int = 20): DataFrame = {
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

    groupByLicense
      .join(sumByLicense, "hack_license")
      .join(avgByLicense, "hack_license")
      .orderBy(desc("$$$$$"))
  }

  def sessionise(df: DataFrame): DataFrame = {
    df.repartition($"hack_license")
      .sortWithinPartitions($"hack_license", $"pickup_datetime")
  }

  def waitTimesByBorough(sessonisedDf: DataFrame): DataFrame = {
    def waitingTime(r1: Row, r2: Row) = {
      (r2.getAs[Timestamp]("pickup_datetime").getTime - r1.getAs[Timestamp]("dropoff_datetime").getTime) / 1000
    }

    val boroughDurations: DataFrame = sessonisedDf.mapPartitions(trips => {
      val iter = trips.sliding(2)

      val viter = iter
        .filter(_.size == 2)
        .filter(p => p.head.getAs("hack_license") == p.tail.head.getAs("hack_license"))

      viter.map(p => (p.head.getAs[String]("dropoff_borough"), waitingTime(p.head, p.tail.head)))
    }).toDF("dropoff_borough", "wait_seconds")

    boroughDurations.
      where("wait_seconds > 0 AND wait_seconds < 60*60*4")
      .groupBy("dropoff_borough")
      .agg(avg("wait_seconds"), stddev("wait_seconds"))
      .withColumnRenamed("avg(wait_seconds)", "average_wait")
      .withColumnRenamed("stddev_samp(wait_seconds)", "stdDev_wait")
  }

}
