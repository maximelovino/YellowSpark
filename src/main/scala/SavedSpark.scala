import org.apache.spark.sql.{SaveMode, SparkSession}
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

  analytics.statsByRateCode(df).show()

  analytics.statsByBoroughPairs(df).show(100)

  analytics.topDrivers(df).show(20)


  val sessions = analytics.sessionise(df)

  sessions.cache()

  val waitTimes = analytics.waitTimesByBorough(sessions)

  waitTimes.write.mode(SaveMode.Overwrite).parquet("./src/main/resources/wait.df")

  waitTimes.show(100, truncate = false)


}
