import org.apache.spark.sql.SparkSession

/**
  * @author Maxime Lovino
  * @date 2019-05-11
  * @package
  * @project YellowSpark
  */

object SavedSpark extends App {
  val spark = SparkSession.builder()
    .appName("Spark Taxi saved")
    .master("local[*]")
    .getOrCreate()

  val df = spark.read.parquet("./src/main/resources/rides.df")

  df.printSchema()

  val analytics = new TaxiAnalytics(spark)

  analytics.displayBoroughStats(df)

  println(s"Number of rides total: ${df.count()}")

  analytics.displayRateCodeStats(df)

  analytics.displayTipStatsByBorough(df)

  analytics.topDrivers(df)
}
