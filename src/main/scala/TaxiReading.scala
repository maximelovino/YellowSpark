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


  val tripsDf = rawTripsDf
    .withColumn("rate_code", col("rate_code").cast(IntegerType))
    .withColumn("pickup_datetime", to_timestamp(col("pickup_datetime"), dateFormat))
    .withColumn("dropoff_datetime", to_timestamp(col("dropoff_datetime"), dateFormat))
    .withColumn("passenger_count", col("passenger_count").cast(IntegerType))
    .withColumn("trip_time_in_secs", col("trip_time_in_secs").cast(IntegerType))
    .withColumn("trip_distance", col("trip_distance").cast(DoubleType))
    .withColumn("pickup_longitude", col("pickup_longitude").cast(DoubleType))
    .withColumn("pickup_latitude", col("pickup_latitude").cast(DoubleType))
    .withColumn("dropoff_longitude", col("dropoff_longitude").cast(DoubleType))
    .withColumn("dropoff_latitude", col("dropoff_latitude").cast(DoubleType))


  tripsDf.printSchema()
  tripsDf.show(2)
  println("=========")
  faresDf.show(2)

  //TODO think about fill null data if there is any


  val df = tripsDf.join(faresDf, Seq("medallion", "hack_license", "pickup_datetime"), "inner")

  df.printSchema()

  val groupByLicense = df.groupBy("hack_license").count()


  val sumByLicense = df.groupBy("hack_license")
    .sum("passenger_count", "trip_distance", "total_amount")
    .withColumnRenamed("sum(passenger_count)", "total_passengers")
    .withColumnRenamed("sum(trip_distance)", "total_distance")
    .withColumnRenamed("sum(total_amount)", "$$$$$")

  val aggByLicense = groupByLicense.join(sumByLicense, "hack_license").orderBy(desc("total_distance"))

  aggByLicense.show(20, truncate = false)

}
