import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.{Pipeline, PipelineModel, linalg}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author Maxime Lovino
  * @date 2019-05-11
  * @package
  * @project YellowSpark
  */

object YellowSparkRegression extends App {
  val spark = SparkSession.builder()
    .appName("Spark Taxi Linear Regression")
    .getOrCreate()

  import spark.implicits._

  val df = spark.read.parquet("s3a://yellowspark-us/rides.df")
  df.printSchema()

  val preparedDF = df
    .select("fare_amount", "rate_code", "trip_time_in_secs", "trip_distance_km", "surcharge", "total_amount", "tolls_amount", "tip_amount")
    .withColumnRenamed("fare_amount", "cost")
    .withColumnRenamed("trip_time_in_secs", "duration")
    .withColumnRenamed("trip_distance_km", "distance")

  preparedDF.printSchema()
  val models = preparedDF.select("rate_code").distinct().where("rate_code BETWEEN 1 and 5").collect().map(row => {
    val rateCode = row.getAs[Int]("rate_code")

    println(s"Training for ratecode: $rateCode")

    val rateCodeSet = preparedDF.where(s"rate_code = $rateCode")

    rateCodeSet.write.mode(SaveMode.Overwrite).parquet(s"s3a://yellowspark-us/rate_code_$rateCode.df")

    val Array(train, test) = rateCodeSet.randomSplit(Array(0.7, 0.3))
    train.write.mode(SaveMode.Overwrite).parquet(s"s3a://yellowspark-us/rate_code_train_$rateCode.df")
    test.write.mode(SaveMode.Overwrite).parquet(s"s3a://yellowspark-us/rate_code_test_$rateCode.df")
    val formula = new RFormula().setFormula("cost ~ duration + distance")

    val reg = new LinearRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")

    val stages = Array(formula, reg)
    val pipeline = new Pipeline().setStages(stages)

    val evaluator = new RegressionEvaluator().setMetricName("mse").setPredictionCol("prediction").setLabelCol("label")


    val fittedModel: PipelineModel = pipeline.fit(train)


    val testPredict = fittedModel.transform(test)
    testPredict.write.mode(SaveMode.Overwrite).parquet(s"s3a://yellowspark-us/predictions_$rateCode.df")
    val mse = evaluator.evaluate(testPredict)

    testPredict.show(10, truncate = false)
    (rateCode, mse, fittedModel)
  })

  models.foreach {
    case (rateCode, mse, fittedPipelineModel) => {
      fittedPipelineModel.write.overwrite().save(s"s3a://yellowspark-us/model_regression_rate_code_$rateCode")
      println(s"Model for rate code $rateCode:")
      val fittedLr = fittedPipelineModel.stages.last.asInstanceOf[LinearRegressionModel]
      val coefficients = fittedLr.coefficients
      val intercept = fittedLr.intercept
      println(s"MSE: $mse")
      println(s"Formula: ${coefficients.apply(0)} * seconds + ${coefficients.apply(1)} * km + $intercept")
    }
  }
  val modelsDF = models.map {
    case (rateCode, mse, fittedPipelineModel) => {
      val fittedLr = fittedPipelineModel.stages.last.asInstanceOf[LinearRegressionModel]
      val coefficients = fittedLr.coefficients
      val intercept = fittedLr.intercept
      val formula = s"${coefficients.apply(0)} * seconds + ${coefficients.apply(1)} * km + $intercept"
      (rateCode, mse, formula, intercept, coefficients.toArray)
    }
  }.toSeq.toDF("rateCode", "mse", "formula", "intercept", "coeffs")

  modelsDF.write.mode(SaveMode.Overwrite).parquet(s"s3a://yellowspark-us/linear_regression_models.df")
}
