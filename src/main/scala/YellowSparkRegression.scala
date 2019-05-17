import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession

/**
  * @author Maxime Lovino
  * @date 2019-05-11
  * @package
  * @project YellowSpark
  */

object YellowSparkRegression extends App {
  val spark = SparkSession.builder()
    .appName("Spark Taxi saved")
    .master("local[*]")
    .getOrCreate()

  val df = spark.read.parquet("./src/main/resources/rides.df")
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

    val Array(train, test) = rateCodeSet.randomSplit(Array(0.7, 0.3))
    val formula = new RFormula().setFormula("cost ~ duration + distance")

    val reg = new LinearRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")

    val stages = Array(formula, reg)
    val pipeline = new Pipeline().setStages(stages)

    val evaluator = new RegressionEvaluator().setMetricName("mse").setPredictionCol("prediction").setLabelCol("label")


    val fittedModel: PipelineModel = pipeline.fit(train)


    val testPredict = fittedModel.transform(test)
    val mse = evaluator.evaluate(testPredict)

    testPredict.show(10, truncate = false)
    (rateCode, mse, fittedModel)
  })

  models.foreach {
    case (rateCode, mse, fittedPipelineModel) => {
      println(s"Model for rate code $rateCode:")
      val fittedLr = fittedPipelineModel.stages.last.asInstanceOf[LinearRegressionModel]
      val coefficients = fittedLr.coefficients
      val intercept = fittedLr.intercept
      println(s"MSE: $mse")
      println(s"Formula: ${coefficients.apply(0)} * seconds + ${coefficients.apply(1)} * km + $intercept")
    }
  }
}
