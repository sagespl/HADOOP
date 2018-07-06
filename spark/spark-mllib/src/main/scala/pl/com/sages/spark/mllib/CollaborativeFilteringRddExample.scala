package pl.com.sages.spark.mllib

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.{SparkConf, SparkContext}
import pl.com.sages.spark.core.BaseSparkApp

object CollaborativeFilteringRddExample extends BaseSparkApp {

  def main(args: Array[String]): Unit = {

    // prepare
    val conf = new SparkConf().setMaster(master).setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    // Load and parse the data
    val data = sc.textFile(sparkAlsTestData)
    val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })

    // Build the recommendation model using ALS
    val rank = 10 // vector size, bigger better but longer, default 10
    val numIterations = 10 // number of iterations
    val lambda = 0.01 // regulation parameter
    val model = ALS.train(ratings, rank, numIterations, lambda)

    // Evaluate the model on rating data
    val usersProducts = ratings.map { case Rating(user, product, rate) => (user, product) }

    val predictions = model.
      predict(usersProducts).
      map { case Rating(user, product, rate) => ((user, product), rate) }

    val ratesAndPreds = ratings.map { case Rating(user, product, rate) => ((user, product), rate) }.
      join(predictions)

    ratesAndPreds.take(10).foreach(println)

    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = r1 - r2
      err * err
    }.mean()
    println("Mean Squared Error = " + MSE)

    // Save and load model
    val outputDir = "/tmp/myCollaborativeFilter"
    FileUtils.deleteDirectory(new File(outputDir))
    model.save(sc, outputDir)
    val sameModel = MatrixFactorizationModel.load(sc, outputDir)

    // stop
    sc.stop()
  }

}