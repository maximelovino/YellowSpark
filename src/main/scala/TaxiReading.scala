import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

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


  val rawDf = spark.read.format("csv").option("header", "true").load("./src/main/resources/trip.csv")

  val dateFormat = "yyyy-MM-dd HH:mm:ss"
  val df = rawDf
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


  df.printSchema()
  df.show(2)

  val groupByLicense = df.groupBy("hack_license").count()


  val sumByLicense = df.groupBy("hack_license")
    .sum("passenger_count","trip_distance")
    .withColumnRenamed("sum(passenger_count)", "total_passengers")
    .withColumnRenamed("sum(trip_distance)", "total_distance")

  val aggByLicense = groupByLicense.join(sumByLicense,"hack_license").orderBy(desc("total_distance"))

  aggByLicense.show(20,truncate = false)

}
