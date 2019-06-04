import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.{DecisionTreeRegressor, GBTRegressor, RandomForestRegressor}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author Maxime Lovino
  * @date 2019-05-17
  * @package
  * @project YellowSpark
  */


//TODO this doesn't work well, built a classificator model with ranges for speed as classes
object YellowSparkCongestion extends App {
  val spark = SparkSession.builder()
    .appName("Spark Taxi Congestion")
    .getOrCreate()

  import spark.implicits._

  val df = spark.read.parquet("s3a://yellowspark-us/rides.df")
    .where("rate_code = 1")
    .where("average_speed_kmh >= 1 and average_speed_kmh < 70")
    .select("pickup_datetime", "dropoff_datetime", "trip_time_in_secs", "trip_distance_km", "average_speed_kmh", "pickup_borough", "dropoff_borough", "pickup_latitude", "pickup_longitude", "dropoff_latitude", "dropoff_longitude")
    .withColumn("pickup_weekday", dayofweek($"pickup_datetime"))
    .withColumn("pickup_hour", hour($"pickup_datetime"))
    .withColumn("pickup_week_hour", $"pickup_weekday" * 24 + $"pickup_hour")
    .drop("pickup_datetime", "dropoff_datetime")


  val pickupMaxHour = df.agg(max("pickup_week_hour")).take(1).head.getInt(0)

  val newDf = df
    .withColumn("sin_pickup_hour_week", sin((lit(2 * Math.PI) * $"pickup_week_hour") / pickupMaxHour))
    .withColumn("cos_pickup_hour_week", cos((lit(2 * Math.PI) * $"pickup_week_hour") / pickupMaxHour))

  val Array(train, test) = newDf.randomSplit(Array(0.7, 0.3))


  val rForm = new RFormula().setFormula("average_speed_kmh ~ cos_pickup_hour_week + sin_pickup_hour_week + pickup_latitude + pickup_longitude + dropoff_latitude + dropoff_longitude")

  val decisionTreeRegressor = new DecisionTreeRegressor().setLabelCol("label").setFeaturesCol("features")
  val rfRegressor = new RandomForestRegressor().setLabelCol("label").setFeaturesCol("features")
  //val gradientBoostedRegressor = new GBTRegressor().setLabelCol("label").setFeaturesCol("features")

  val paramGrid = new ParamGridBuilder()
    .addGrid(rForm.formula, Array(
      "average_speed_kmh ~ pickup_week_hour + pickup_borough + dropoff_borough",
      "average_speed_kmh ~ sin_pickup_hour_week + pickup_borough + dropoff_borough",
      "average_speed_kmh ~ cos_pickup_hour_week + pickup_borough + dropoff_borough",
      "average_speed_kmh ~ cos_pickup_hour_week:sin_pickup_hour_week + pickup_borough + dropoff_borough",
      "average_speed_kmh ~ cos_pickup_hour_week + sin_pickup_hour_week + pickup_borough + dropoff_borough",
      "average_speed_kmh ~ pickup_week_hour + pickup_latitude + pickup_longitude + dropoff_latitude + dropoff_longitude",
      "average_speed_kmh ~ sin_pickup_hour_week + pickup_latitude + pickup_longitude + dropoff_latitude + dropoff_longitude",
      "average_speed_kmh ~ cos_pickup_hour_week + pickup_latitude + pickup_longitude + dropoff_latitude + dropoff_longitude",
      "average_speed_kmh ~ cos_pickup_hour_week:sin_pickup_hour_week + pickup_latitude + pickup_longitude + dropoff_latitude + dropoff_longitude",
      "average_speed_kmh ~ cos_pickup_hour_week + sin_pickup_hour_week + pickup_latitude + pickup_longitude + dropoff_latitude + dropoff_longitude"
    )).build()


  val regressors = Array(("DecisionTree", decisionTreeRegressor), ("RandomForest", rfRegressor))


  val regressorsResult = regressors.map {
    case (name, regressor) => {
      println(s"Training $name")
      val pipeline = new Pipeline()
        .setStages(Array(rForm, regressor))

      val evaluator = new RegressionEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setMetricName("mse")

      /*val cv = new CrossValidator()
        .setEstimator(pipeline)
        .setEvaluator(evaluator)
        .setEstimatorParamMaps(paramGrid)
        .setNumFolds(5)
        .setParallelism(8)*/

      //val fittedModel = cv.fit(train)
      val fittedModel = pipeline.fit(train)

      val predictions = fittedModel.transform(test)
      predictions.show(10)

      val mse = evaluator.evaluate(predictions)
      println(s"Mean Squared Error (MSE) on test data = $mse")
      (name, fittedModel, mse)
    }
  }

  val regressorsDf = regressorsResult.map {
    case (name, model, mse) => (name, mse)
  }.toSeq.toDF("model_type", "MSE")

  regressorsDf.write.mode(SaveMode.Overwrite).parquet("s3a://yellowspark-us/congestion_models.df")

  regressorsResult.foreach {
    case (name, model, mse) => {
      println(s"$name => MSE: $mse")
      model.write.overwrite().save(s"s3a://yellowspark-us/congestion_model_$name.df")
    }
  }
}
