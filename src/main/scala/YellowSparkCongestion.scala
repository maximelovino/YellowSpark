import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @author Maxime Lovino
  * @date 2019-05-17
  * @package
  * @project YellowSpark
  */

object YellowSparkCongestion extends App {
  val spark = SparkSession.builder()
    .appName("Spark Taxi Congestion")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val df = spark.read.parquet("./src/main/resources/rides.df")
    .where("rate_code = 1")
    .select("pickup_datetime", "dropoff_datetime", "trip_time_in_secs", "trip_distance_km", "average_speed_kmh", "pickup_borough", "dropoff_borough")
    .where("average_speed_kmh > 0.0")
    .where("average_speed_kmh < 100.0")
    .where("trip_distance_km > 1")
    .where("trip_time_in_secs > 30")
    .withColumn("pickup_weekday", dayofweek($"pickup_datetime"))
    .withColumn("pickup_hour", hour($"pickup_datetime"))
    .withColumn("dropoff_weekday", dayofweek($"dropoff_datetime"))
    .withColumn("dropoff_hour", hour($"dropoff_datetime"))
    .drop("pickup_datetime", "dropoff_datetime")


  val Array(train, test) = df.randomSplit(Array(0.7, 0.3))


  val pickupIndexer = new StringIndexer()
    .setInputCol("pickup_borough")
    .setOutputCol("pickup_borough_index")

  val dropoffIndexer = new StringIndexer()
    .setInputCol("dropoff_borough")
    .setOutputCol("dropoff_borough_index")

  val encoder = new OneHotEncoderEstimator()
    .setInputCols(Array("pickup_weekday", "pickup_hour", "dropoff_weekday", "dropoff_hour", "pickup_borough_index", "dropoff_borough_index"))
    .setOutputCols(Array("pickup_weekday_vec", "pickup_hour_vec", "dropoff_weekday_vec", "dropoff_hour_vec", "pickup_borough_vec", "dropoff_borough_vec"))

  val rFormula = new RFormula().setFormula("average_speed_kmh ~ pickup_weekday_vec + pickup_hour_vec + dropoff_weekday_vec + dropoff_hour_vec + pickup_borough_vec + dropoff_borough_vec")


  val regressor = new RandomForestRegressor().setLabelCol("label").setFeaturesCol("features")


  val pipeline = new Pipeline()
    .setStages(Array(pickupIndexer, dropoffIndexer, encoder, rFormula, regressor))


  val fittedModel = pipeline.fit(train)


  val predictions = fittedModel.transform(test)
  predictions.show(10)

  val evaluator = new RegressionEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("rmse")
  val rmse = evaluator.evaluate(predictions)
  println(s"Root Mean Squared Error (RMSE) on test data = $rmse")
}
