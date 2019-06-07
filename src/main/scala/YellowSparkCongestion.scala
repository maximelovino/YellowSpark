import config.Constants
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author Maxime Lovino
  * @date 2019-05-17
  * @package
  * @project YellowSpark
  */


object YellowSparkCongestion extends App {
  val spark = SparkSession.builder()
    .appName("Spark Taxi Congestion")
    .getOrCreate()

  import spark.implicits._

  val df = spark.read.parquet(s"${Constants.rootFolderScheme}/rides_final.df")
    .where("rate_code = 1")
    .where("average_speed_kmh < 70")
    .select("pickup_datetime", "dropoff_datetime", "trip_time_in_secs", "trip_distance_km", "average_speed_kmh", "pickup_borough", "dropoff_borough", "pickup_latitude", "pickup_longitude", "dropoff_latitude", "dropoff_longitude")
    .withColumn("month", month($"pickup_datetime"))
    .withColumn("pickup_weekday", dayofweek($"pickup_datetime"))
    .withColumn("pickup_hour", hour($"pickup_datetime"))
    .withColumn("pickup_week_hour", $"pickup_weekday" * 24 + $"pickup_hour")
    .withColumn("speed_class", floor($"average_speed_kmh" / 10).as[Int])
    .drop("pickup_datetime", "dropoff_datetime", "pickup_weekday", "dropoff_weekday", "pickup_hour", "dropoff_hour")
    .filter("pickup_borough = 'Manhattan' and dropoff_borough = 'Manhattan'")

  val pickupMaxHour = df.agg(max("pickup_week_hour")).take(1).head.getInt(0)

  val newDf = df
    .withColumn("sin_pickup_hour_week", sin((lit(2 * Math.PI) * $"pickup_week_hour") / pickupMaxHour))
    .withColumn("cos_pickup_hour_week", cos((lit(2 * Math.PI) * $"pickup_week_hour") / pickupMaxHour))


  newDf.printSchema()
  println(newDf.count())

  newDf.show(10)

  val Array(train, test) = newDf.randomSplit(Array(0.7, 0.3))

  val rForm = new RFormula().setFormula("trip_time_in_secs ~ cos_pickup_hour_week + sin_pickup_hour_week + pickup_latitude + pickup_longitude + dropoff_latitude + dropoff_longitude + trip_distance_km")


  val regressor = new GBTRegressor().setLabelCol("label").setFeaturesCol("features")


  val pipeline = new Pipeline()
    .setStages(Array(rForm, regressor))

  val evaluator = new RegressionEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("mse")

  val fittedModel = pipeline.fit(train)

  val predictions = fittedModel.transform(test)
  val predictionsSelected = predictions.select("prediction", "label")
  predictionsSelected.show(10)

  val mse = evaluator.evaluate(predictions)
  println(s"Mean Squared Error (MSE) on test data = $mse")

  fittedModel.write.overwrite().save(s"${Constants.rootFolderScheme}/congestion_model_gradient_boost.df")
  predictions.write.mode(SaveMode.Overwrite).parquet(s"${Constants.rootFolderScheme}/predictions_congestion.df")
}
