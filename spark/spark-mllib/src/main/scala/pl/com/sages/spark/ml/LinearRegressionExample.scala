package pl.com.sages.spark.ml

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession
import pl.com.sages.spark.core.BaseSparkApp

object LinearRegressionExample extends BaseSparkApp {

  def main(args: Array[String]): Unit = {

    // prepare
    val spark = SparkSession.builder.master(master).appName(this.getClass.getSimpleName).getOrCreate()

    // Load training data
    val training = spark.read.format("libsvm")
      .load(sparkSampleLinearRegressionData)
    training.show(10)

    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model
    val lrModel = lr.fit(training)
    lrModel.evaluate(training).predictions.show(10)

    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

    // stop
    spark.stop()
  }
}