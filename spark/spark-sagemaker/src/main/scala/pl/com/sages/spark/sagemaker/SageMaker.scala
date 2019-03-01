package pl.com.sages.spark.sagemaker

import com.amazonaws.services.sagemaker.sparksdk.IAMRole
import com.amazonaws.services.sagemaker.sparksdk.algorithms.KMeansSageMakerEstimator
import org.apache.spark.sql.SparkSession
import pl.com.sages.spark.core.BaseSparkApp
import pl.com.sages.spark.sql.BaseSparkSqlApp

object SageMaker extends BaseSparkSqlApp with BaseSparkApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.getOrCreate

    // load mnist data as a dataframe from libsvm
    val region = "us-east-1"
    val trainingData = spark.read.format("libsvm")
      .option("numFeatures", "784")
      .load(s"s3://sagemaker-sample-data-$region/spark/mnist/train/")
    val testData = spark.read.format("libsvm")
      .option("numFeatures", "784")
      .load(s"s3://sagemaker-sample-data-$region/spark/mnist/test/")

    //    val roleArn = "arn:aws:iam::account-id:role/rolename"
    val roleArn = "arn:aws:iam::027803631740:role/PWSageMaker"
    //val roleArn = "arn:aws:iam::027803631740:role/EMR_Notebooks_DefaultRole"

    val estimator = new KMeansSageMakerEstimator(
      sagemakerRole = IAMRole(roleArn),
      trainingInstanceType = "ml.p2.xlarge",
      trainingInstanceCount = 1,
      endpointInstanceType = "ml.c4.xlarge",
      endpointInitialInstanceCount = 1)
      .setK(10).setFeatureDim(784)

    // train
    val start = System.currentTimeMillis()
    val model = estimator.fit(trainingData)
    val stop = System.currentTimeMillis()

    val time = (stop - start)/1000/60
    time

    val transformedData = model.transform(testData)
    transformedData.show

  }

}