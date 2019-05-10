import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author Maxime Lovino
  * @date 2019-05-06
  * @package
  * @project YellowSpark
  */


object TaxiReading extends App {
  val MILES_TO_KM = 1.60934

  val spark = SparkSession.builder()
    .appName("Spark Taxi example")
    .master("local[*]")
    .getOrCreate()

  def trimmedDataFrame(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df)((curr, col) => curr.withColumnRenamed(col, col.trim()))
  }

  val rawTripsDf = spark.read.format("csv").option("header", "true").load("./src/main/resources/trip.csv")
  val rawFaresDf = spark.read.format("csv").option("header", "true").load("./src/main/resources/fare.csv")


  val cleanedUpDf = trimmedDataFrame(rawFaresDf) // The fares CSV contains spaces in headers so we have to trim the column names
  cleanedUpDf.printSchema()
  val dateFormat = "yyyy-MM-dd HH:mm:ss"

  val faresDf = cleanedUpDf
    .withColumn("pickup_datetime", to_timestamp(col("pickup_datetime"), dateFormat))
    .withColumn("fare_amount", col("fare_amount").cast(DoubleType))
    .withColumn("surcharge", col("surcharge").cast(DoubleType))
    .withColumn("mta_tax", col("mta_tax").cast(DoubleType))
    .withColumn("tip_amount", col("tip_amount").cast(DoubleType))
    .withColumn("tolls_amount", col("tolls_amount").cast(DoubleType))
    .withColumn("total_amount", col("total_amount").cast(DoubleType))
    .withColumn("taxi_revenue", col("tip_amount") + col("fare_amount"))


  val tripsDf = rawTripsDf
    .withColumn("rate_code", col("rate_code").cast(IntegerType))
    .withColumn("pickup_datetime", to_timestamp(col("pickup_datetime"), dateFormat))
    .withColumn("dropoff_datetime", to_timestamp(col("dropoff_datetime"), dateFormat))
    .withColumn("passenger_count", col("passenger_count").cast(IntegerType))
    .withColumn("trip_time_in_secs", col("trip_time_in_secs").cast(IntegerType))
    .withColumn("trip_distance", col("trip_distance").cast(DoubleType))
    .withColumn("trip_distance_km", col("trip_distance") * MILES_TO_KM)
    .withColumn("average_speed_kmh", (col("trip_distance_km") / col("trip_time_in_secs")) * 3600)
    .withColumn("pickup_longitude", col("pickup_longitude").cast(DoubleType))
    .withColumn("pickup_latitude", col("pickup_latitude").cast(DoubleType))
    .withColumn("dropoff_longitude", col("dropoff_longitude").cast(DoubleType))
    .withColumn("dropoff_latitude", col("dropoff_latitude").cast(DoubleType))


  tripsDf.printSchema()
  tripsDf.show(2)
  println("=========")
  faresDf.show(2)


  val df = tripsDf.join(faresDf, Seq("medallion", "hack_license", "pickup_datetime"), "inner")


  val finalDf = df.filter(col("rate_code") !== 5)
    .filter(col("trip_time_in_secs") > 1)
    .filter(col("trip_distance") > 0.0)
    .filter(col("pickup_longitude") < -70.0 && col("pickup_longitude") > -75.0)
    .filter(col("dropoff_longitude") < -70.0 && col("dropoff_longitude") > -75.0)
    .filter(col("pickup_latitude") > 40.0 && col("pickup_latitude") < 42.0)
    .filter(col("dropoff_latitude") > 40.0 && col("dropoff_latitude") < 42.0)
    .filter(col("average_speed_kmh") < 150)
    .filter(col("trip_distance_km") >= 1)
    .filter(col("trip_time_in_secs") >= 30)


  finalDf.cache()

  finalDf.orderBy("trip_time_in_secs").limit(20).show(truncate = false)

  val groupByLicense = finalDf.groupBy("hack_license").count()
  val avgByLicense = finalDf.groupBy("hack_license").avg("average_speed_kmh")
    .withColumnRenamed("avg(average_speed_kmh)", "Average speed in km/h")


  val sumByLicense = finalDf.groupBy("hack_license")
    .sum("passenger_count", "trip_distance_km", "taxi_revenue", "tip_amount", "fare_amount", "trip_time_in_secs")
    .withColumnRenamed("sum(passenger_count)", "total_passengers")
    .withColumnRenamed("sum(trip_distance_km)", "total_distance_km")
    .withColumnRenamed("sum(taxi_revenue)", "$$$$$")
    .withColumnRenamed("sum(tip_amount)", "TIPS")
    .withColumnRenamed("sum(fare_amount)", "FARES")
    .withColumnRenamed("sum(trip_time_in_secs)", "DURATION")

  val aggByLicense = groupByLicense.join(sumByLicense, "hack_license").join(avgByLicense, "hack_license").orderBy(desc("$$$$$"))

  aggByLicense.show(20, truncate = false)

}
