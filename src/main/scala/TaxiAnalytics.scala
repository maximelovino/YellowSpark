import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.{DataFrame, SparkSession}

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

  def displayRateCodeStats(df: DataFrame): Unit = {
    df.groupBy($"rate_code").count().show()
  }

  def displayTipStatsByBorough(df: DataFrame): Unit = {
    df.groupBy($"pickup_borough").avg("tip_amount").show()
    df.groupBy($"dropoff_borough").avg("tip_amount").show()
    val avgTip = df.groupBy($"pickup_borough", $"dropoff_borough").avg("tip_amount")
    val count = df.groupBy($"pickup_borough", $"dropoff_borough").count()
    avgTip.join(count, Seq("pickup_borough", "dropoff_borough"))
      .orderBy("pickup_borough", "dropoff_borough")
      .show(100, truncate = false)
  }


  def topDrivers(df: DataFrame, count: Int = 20) = {
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

}
