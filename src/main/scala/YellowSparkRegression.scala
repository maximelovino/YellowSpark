import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.regression.LinearRegression
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
    .withColumnRenamed("fare_amount", "cost")
    .withColumnRenamed("trip_time_in_secs", "duration")
    .withColumnRenamed("trip_distance_km", "distance")

  df.printSchema()

  org.apache.spark.ml.regression.LinearRegression
  val rateCode1 = df.where("rate_code = 1")
  val formula = new RFormula().setFormula("cost ~ duration + distance")

  val fitFormula = formula.fit(rateCode1)
  val preparedDF = fitFormula.transform(rateCode1)

  val Array(train, test) = preparedDF.randomSplit(Array(0.7, 0.3))

  val reg = new LinearRegression().setLabelCol("label").setFeaturesCol("features")
  val fittedReg = reg.fit(train)

  println(s"Coefficients: ${fittedReg.coefficients} Intercept: ${fittedReg.intercept}")

  val evaluator = new RegressionEvaluator().setMetricName("mse").setPredictionCol("prediction").setLabelCol("label")
  println(s"MSE: ${evaluator.evaluate(fittedReg.transform(test))}")
}
