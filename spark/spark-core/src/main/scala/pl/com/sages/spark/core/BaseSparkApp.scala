package pl.com.sages.spark.core

import org.apache.spark.{SparkConf, SparkContext}

trait BaseSparkApp extends ClusterApp {

  /**
    * Port and hostname for Spark Streaming and TCP connection
    */
  val hostname: String = "localhost"
  val port: String = "9999"

  /**
    * Kafka params for Spark Streaming
    */
  val kafkaBootstrapServers = "cluster_kafka1:9092,cluster_kafka2:9092,cluster_kafka3:9092"
  val kafkaTopics = "test-topic"
  val kafkaGroupId = "spark-streaming"

  /**
    * Wolne Lektury dataset
    */
  val bookPath: String = dataPath + "/lektury/lektury-all"

  /**
    * Movielens dataset
    */
  val moviesPath: String = dataPath + "/movielens/hive/movies/movies.dat"
  val tagsPath: String = dataPath + "/movielens/hive/tags/tags.dat"
  val ratingsPath: String = dataPath + "/movielens/hive/ratings/ratings.dat"
  val movielensSeparator: String = "@"

  /**
    * Spark test data
    */
  val sparkSampleData: String = dataPath + "/spark-data"
  val sparkSampleLibsvmData: String = sparkSampleData + "/mllib/sample_libsvm_data.txt"
  val sparkSampleMovielensRatingsData: String = sparkSampleData + "/mllib/als/sample_movielens_ratings.txt"
  val sparkAlsTestData: String = sparkSampleData + "/mllib/als/test.data"
  val sparkKMeansData: String = sparkSampleData + "/mllib/kmeans_data.txt"
  val sparkSampleKMeansData: String = sparkSampleData + "/mllib/sample_kmeans_data.txt"
  val sparkSampleLinearRegressionData: String = sparkSampleData + "/mllib/sample_linear_regression_data.txt"

  def createSparkContext: SparkContext = {

    val conf = new SparkConf().
      setMaster(master).
      setAppName(user + " " + this.getClass.getSimpleName)

    val sc = new SparkContext(conf)

    sc
  }

}
