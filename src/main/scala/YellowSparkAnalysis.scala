import java.sql.Timestamp

import org.apache.spark.sql.functions.{avg, desc, stddev, _}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/**
  * @author Maxime Lovino
  * @date 2019-05-11
  * @package
  * @project YellowSpark
  */

object YellowSparkAnalysis extends App {
  val spark = SparkSession.builder()
    .appName("Spark Taxi saved")
    .getOrCreate()

  import spark.implicits._

  def statsByRateCode(df: DataFrame): DataFrame = {
    df.groupBy($"rate_code").count()
  }

  def statsByPickupBorough(df: DataFrame): DataFrame = {
    statsForGrouping("pickup_borough", df)
  }

  def statsByDropoffBorough(df: DataFrame): DataFrame = {
    statsForGrouping("dropoff_borough", df)
  }

  def statsByBoroughPairs(df: DataFrame): DataFrame = {
    val avg = df.groupBy($"pickup_borough", $"dropoff_borough").avg("tip_amount", "trip_time_in_secs", "trip_distance_km", "taxi_revenue")
      .withColumnRenamed("avg(tip_amount)", "avg_tip_amount")
      .withColumnRenamed("avg(trip_time_in_secs)", "avg_trip_time_in_secs")
      .withColumnRenamed("avg(trip_distance_km)", "avg_trip_distance_km")
      .withColumnRenamed("avg(taxi_revenue)", "avg_taxi_revenue")
    val sum = df.groupBy($"pickup_borough", $"dropoff_borough").sum("trip_time_in_secs", "trip_distance_km", "taxi_revenue")
      .withColumnRenamed("sum(trip_time_in_secs)", "sum_trip_time_in_secs")
      .withColumnRenamed("sum(trip_distance_km)", "sum_trip_distance_km")
      .withColumnRenamed("sum(taxi_revenue)", "sum_taxi_revenue")
    val count = df.groupBy($"pickup_borough", $"dropoff_borough").count()
    avg.join(count, Seq("pickup_borough", "dropoff_borough")).join(sum, Seq("pickup_borough", "dropoff_borough"))
      .orderBy("pickup_borough", "dropoff_borough")
  }


  def topDrivers(df: DataFrame): DataFrame = {
    val groupByLicense = df.groupBy("hack_license").count()
    val avgByLicense = df.groupBy("hack_license").avg("average_speed_kmh")
      .withColumnRenamed("avg(average_speed_kmh)", "global_average_speed_kmh")

    val sumByLicense = df.groupBy("hack_license")
      .sum("passenger_count", "trip_distance_km", "taxi_revenue", "tip_amount", "fare_amount", "trip_time_in_secs")
      .withColumnRenamed("sum(passenger_count)", "total_passengers")
      .withColumnRenamed("sum(trip_distance_km)", "total_distance_km")
      .withColumnRenamed("sum(taxi_revenue)", "total_revenue")
      .withColumnRenamed("sum(tip_amount)", "total_tips")
      .withColumnRenamed("sum(fare_amount)", "total_fares")
      .withColumnRenamed("sum(trip_time_in_secs)", "total_duration")

    groupByLicense
      .join(sumByLicense, "hack_license")
      .join(avgByLicense, "hack_license")
      .orderBy(desc("total_revenue"))
  }

  def topMedallions(df: DataFrame): DataFrame = {
    val groupByLicense = df.groupBy("medallion").count()
    val avgByLicense = df.groupBy("medallion").avg("average_speed_kmh")
      .withColumnRenamed("avg(average_speed_kmh)", "global_average_speed_kmh")

    val sumByLicense = df.groupBy("medallion")
      .sum("passenger_count", "trip_distance_km", "taxi_revenue", "tip_amount", "fare_amount", "trip_time_in_secs")
      .withColumnRenamed("sum(passenger_count)", "total_passengers")
      .withColumnRenamed("sum(trip_distance_km)", "total_distance_km")
      .withColumnRenamed("sum(taxi_revenue)", "total_revenue")
      .withColumnRenamed("sum(tip_amount)", "total_tips")
      .withColumnRenamed("sum(fare_amount)", "total_fares")
      .withColumnRenamed("sum(trip_time_in_secs)", "total_duration")

    groupByLicense
      .join(sumByLicense, "medallion")
      .join(avgByLicense, "medallion")
      .orderBy(desc("total_distance_km"))
  }

  def sessionise(df: DataFrame): DataFrame = {
    df.repartition($"hack_license")
      .sortWithinPartitions($"hack_license", $"pickup_datetime")
  }

  private def waitingTime(r1: Row, r2: Row) = {
    (r2.getAs[Timestamp]("pickup_datetime").getTime - r1.getAs[Timestamp]("dropoff_datetime").getTime) / 1000
  }


  private def mapBoroughs(trips: Iterator[Row]) = {
    val iter = trips.sliding(2)

    val viter = iter
      .filter(_.size == 2)
      .filter(p => p.head.getAs("hack_license") == p.tail.head.getAs("hack_license"))

    viter.map(p => (p.head.getAs[String]("dropoff_borough"), waitingTime(p.head, p.tail.head)))
  }

  def waitTimesByBorough(sessonisedDf: DataFrame): DataFrame = {
    val boroughDurations: DataFrame = sessonisedDf.mapPartitions(mapBoroughs).toDF("dropoff_borough", "wait_seconds")

    boroughDurations.
      where("wait_seconds > 0 AND wait_seconds < 60*60*4")
      .groupBy("dropoff_borough")
      .agg(avg("wait_seconds"), stddev("wait_seconds"))
      .withColumnRenamed("avg(wait_seconds)", "average_wait")
      .withColumnRenamed("stddev_samp(wait_seconds)", "stdDev_wait")
  }


  def statsByDayYear(df: DataFrame): DataFrame = {
    val dfWithDay = df.withColumn("day_year", dayofyear($"pickup_datetime"))

    statsForGrouping("day_year", dfWithDay)
  }

  def statsByDayWeek(df: DataFrame): DataFrame = {
    val dfWithDay = df.withColumn("day_week", dayofweek($"pickup_datetime"))

    statsForGrouping("day_week", dfWithDay)

  }

  def statsByMonth(df: DataFrame): DataFrame = {
    val dfWithMonth = df.withColumn("month", month($"pickup_datetime"))
    statsForGrouping("month", dfWithMonth)
  }

  def statsByHour(df: DataFrame): DataFrame = {
    val dfWithHour = df.withColumn("hour", hour($"pickup_datetime"))

    statsForGrouping("hour", dfWithHour)
  }


  def distanceBinsStats(df: DataFrame): DataFrame = {
    val dfBins = df
      .withColumn("distance_bin", floor($"trip_distance_km").as[Int])

    statsForGrouping("distance_bin", dfBins)
  }


  def speedBinsStats(df: DataFrame): DataFrame = {
    val dfBins = df
      .withColumn("speed_class", floor($"average_speed_kmh" / 5).as[Int])

    statsForGrouping("speed_class", dfBins)
  }


  def durationBinsStats(df: DataFrame): DataFrame = {
    val dfBins = df
      .withColumn("duration_class", floor($"trip_time_in_secs" / 30).as[Int])

    statsForGrouping("duration_class", dfBins)
  }

  def revenueBinStats(df: DataFrame): DataFrame = {
    val dfBins = df
      .withColumn("revenue_bin", floor($"taxi_revenue" / 5).as[Int])

    statsForGrouping("revenue_bin", dfBins)
  }


  private def statsForGrouping(groupCol: String, df: DataFrame): DataFrame = {
    val count = df.groupBy(groupCol).count()

    val avg = df.groupBy(groupCol)
      .avg("tip_amount", "trip_time_in_secs", "trip_distance_km", "taxi_revenue")
      .withColumnRenamed("avg(tip_amount)", "avg_tip_amount")
      .withColumnRenamed("avg(trip_time_in_secs)", "avg_trip_time_in_secs")
      .withColumnRenamed("avg(trip_distance_km)", "avg_trip_distance_km")
      .withColumnRenamed("avg(taxi_revenue)", "avg_taxi_revenue")

    val sum = df.groupBy(groupCol)
      .sum("trip_time_in_secs", "trip_distance_km", "taxi_revenue", "passenger_count")
      .withColumnRenamed("sum(trip_time_in_secs)", "sum_trip_time_in_secs")
      .withColumnRenamed("sum(trip_distance_km)", "sum_trip_distance_km")
      .withColumnRenamed("sum(taxi_revenue)", "sum_taxi_revenue")
      .withColumnRenamed("sum(passenger_count)", "sum_passengers")

    avg.join(count, groupCol).join(sum, groupCol)
      .orderBy(groupCol)
  }


  val df = spark.read.parquet("s3a://yellowspark-us-new/rides.df")

  df.printSchema()

  println(s"Number of rides total: ${df.count()}")

  val rateCodeStats = statsByRateCode(df)
  rateCodeStats.write.mode(SaveMode.Overwrite).parquet("s3a://yellowspark-us-new/rateCodes.df")
  rateCodeStats.show()


  println("Doing pickupStats...")
  val pickupStats = statsByPickupBorough(df)
  pickupStats.write.mode(SaveMode.Overwrite).parquet("s3a://yellowspark-us-new/pickups.df")

  println("Doing dropoffStats...")
  val dropoffStats = statsByDropoffBorough(df)
  dropoffStats.write.mode(SaveMode.Overwrite).parquet("s3a://yellowspark-us-new/dropoffs.df")

  println("Doing boroughPairs...")
  val boroughPairs = statsByBoroughPairs(df)
  boroughPairs.write.mode(SaveMode.Overwrite).parquet("s3a://yellowspark-us-new/boroughPairs.df")

  println("Doing topDriversDf...")
  val topDriversDf = topDrivers(df)
  topDriversDf.write.mode(SaveMode.Overwrite).parquet("s3a://yellowspark-us-new/topDrivers.df")

  println("Doing topMedallionsDf...")
  val topMedallionsDf = topMedallions(df)
  topMedallionsDf.write.mode(SaveMode.Overwrite).parquet("s3a://yellowspark-us-new/topMedallions.df")

  println("Doing daysDf...")
  val daysDf = statsByDayYear(df)
  daysDf.write.mode(SaveMode.Overwrite).parquet("s3a://yellowspark-us-new/dayStats.df")

  println("Doing daysWeekDf...")
  val daysWeekDf = statsByDayWeek(df)
  daysWeekDf.write.mode(SaveMode.Overwrite).parquet("s3a://yellowspark-us-new/dayWeekStats.df")

  println("Doing monthsDf...")
  val monthsDf = statsByMonth(df)
  monthsDf.write.mode(SaveMode.Overwrite).parquet("s3a://yellowspark-us-new/monthStats.df")

  println("Doing hoursDf...")
  val hoursDf = statsByHour(df)
  hoursDf.write.mode(SaveMode.Overwrite).parquet("s3a://yellowspark-us-new/hourStats.df")

  println("Doing distancesDf...")
  val distancesDf = distanceBinsStats(df)
  distancesDf.write.mode(SaveMode.Overwrite).parquet("s3a://yellowspark-us-new/distStats.df")

  println("Doing speedsDf...")
  val speedsDf = speedBinsStats(df)
  speedsDf.write.mode(SaveMode.Overwrite).parquet("s3a://yellowspark-us-new/speedStats.df")

  println("Doing durationsDf...")
  val durationsDf = durationBinsStats(df)
  durationsDf.write.mode(SaveMode.Overwrite).parquet("s3a://yellowspark-us-new/durationStats.df")

  println("Doing revenueDf...")
  val revenueDf = revenueBinStats(df)
  revenueDf.write.mode(SaveMode.Overwrite).parquet("s3a://yellowspark-us-new/revenueStats.df")

  println("Doing sessions...")
  val sessions = sessionise(df)

  println("Doing waitTimes...")
  val waitTimes = waitTimesByBorough(sessions)
  waitTimes.write.mode(SaveMode.Overwrite).parquet("s3a://yellowspark-us-new/waitTimes.df")

}
