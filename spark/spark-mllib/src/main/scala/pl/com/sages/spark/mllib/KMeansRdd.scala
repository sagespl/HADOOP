package pl.com.sages.spark.mllib

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import pl.com.sages.spark.GlobalMlParameters

object KMeansRdd extends GlobalMlParameters {

  def main(args: Array[String]) {

    // prepare
    val conf = new SparkConf().setMaster(master).setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    // Load and parse the data
    val data = sc.textFile(sparkKMeansData)
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    // Save and load model
    val outputDir = "/tmp/KMeansExample/KMeansModel"
    FileUtils.deleteDirectory(new File(outputDir))
    clusters.save(sc, outputDir)
    val sameModel = KMeansModel.load(sc, outputDir)

    // stop
    sc.stop()
  }
}
