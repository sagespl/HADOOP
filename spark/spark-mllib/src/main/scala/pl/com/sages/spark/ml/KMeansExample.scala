package pl.com.sages.spark.ml

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.SparkSession
import pl.com.sages.spark.core.BaseSparkApp

object KMeansExample extends BaseSparkApp {

  def main(args: Array[String]): Unit = {

    // prepare
    val spark = SparkSession.builder.master(master).appName(this.getClass.getSimpleName).getOrCreate()

    // Loads data.
    val dataset = spark.read.format("libsvm").load(sparkSampleKMeansData)

    dataset.show(10, truncate = false)

    // Trains a k-means model.
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(dataset)

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model.computeCost(dataset)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

    println("Prediction: ")
    val transformed =  model.transform(dataset)
    transformed.show(10, truncate = false)

    println("Prediction2: ")
    model.summary.predictions.show

    // stop
    spark.stop()
  }
}
