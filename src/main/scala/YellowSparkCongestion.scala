import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.{DecisionTreeRegressor, RandomForestRegressor}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
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
    .where("average_speed_kmh >= 1 and average_speed_kmh < 70")
    .select("pickup_datetime", "dropoff_datetime", "trip_time_in_secs", "trip_distance_km", "average_speed_kmh", "pickup_borough", "dropoff_borough", "pickup_latitude", "pickup_longitude", "dropoff_latitude", "dropoff_longitude")
    .withColumn("pickup_weekday", dayofweek($"pickup_datetime"))
    .withColumn("pickup_hour", hour($"pickup_datetime"))
    .withColumn("dropoff_weekday", dayofweek($"dropoff_datetime"))
    .withColumn("dropoff_hour", hour($"dropoff_datetime"))
    .withColumn("pickup_week_hour", $"pickup_weekday" * 24 + $"pickup_hour")
    .withColumn("dropoff_week_hour", $"dropoff_weekday" * 24 + $"dropoff_hour")
    .drop("pickup_datetime", "dropoff_datetime")

  val Array(train, test) = df.randomSplit(Array(0.7, 0.3))


  /*val pickupIndexer = new StringIndexer()
    .setInputCol("pickup_borough")
    .setOutputCol("pickup_borough_index")

  val dropoffIndexer = new StringIndexer()
    .setInputCol("dropoff_borough")
    .setOutputCol("dropoff_borough_index")

  val encoder = new OneHotEncoderEstimator()
    .setInputCols(Array("pickup_borough_index", "dropoff_borough_index"))
    .setOutputCols(Array("pickup_borough_vec", "dropoff_borough_vec"))*/

  val rFormula = new RFormula().setFormula("average_speed_kmh ~ pickup_week_hour + dropoff_week_hour + pickup_borough + dropoff_borough")


  val regressor = new DecisionTreeRegressor().setLabelCol("label").setFeaturesCol("features")


  val pipeline = new Pipeline()
    .setStages(Array(rFormula, regressor))

  val evaluator = new RegressionEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("mse")


  /*val paramGrid = new ParamGridBuilder()
    .addGrid(regressor.numTrees)


  val cv = new CrossValidator()
    .setEstimator(pipeline)
    .setEvaluator(evaluator)
    .setEstimatorParamMaps(paramGrid)
    .setNumFolds(10) // Use 3+ in practice
    .setParallelism(8) // Evaluate up to 2 parameter settings in parallel*/

  val fittedModel = pipeline.fit(train)


  val predictions = fittedModel.transform(test)
  predictions.show(10)

  val mse = evaluator.evaluate(predictions)
  println(s"Mean Squared Error (MSE) on test data = $mse")
}
