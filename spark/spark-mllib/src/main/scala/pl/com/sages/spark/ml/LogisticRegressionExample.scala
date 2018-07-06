package pl.com.sages.spark.ml

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession
import pl.com.sages.spark.core.BaseSparkApp

object LogisticRegressionExample extends BaseSparkApp {

  def main(args: Array[String]): Unit = {

    // prepare
    val spark = SparkSession.builder.master("local").appName(this.getClass.getSimpleName).getOrCreate()

    val training = spark.read.format("libsvm").load(sparkSampleLibsvmData)
    training.show(10)

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model
    val lrModel = lr.fit(training)
    lrModel.evaluate(training).predictions.show(10)

    // Print the coefficients and intercept for logistic regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // We can also use the multinomial family for binary classification
    val mlr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setFamily("multinomial")

    val mlrModel = mlr.fit(training)

    // Print the coefficients and intercepts for logistic regression with multinomial family
    println(s"Multinomial coefficients: ${mlrModel.coefficientMatrix}")
    println(s"Multinomial intercepts: ${mlrModel.interceptVector}")

    // stop
    spark.stop()
  }

}
