package pl.com.sages.spark.core

import org.apache.spark.{SparkConf, SparkContext}

trait BaseSparkApp {

  /**
    * User name
    */
  val user: String = System.getProperty("user.name")

  /**
    * Where to run master process
    */
  val master: String = "local"
  // val master: String = "local[4]"
  // val master: String = "yarn"

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
    * File system schema: local, Hadoop or Amazon S3
    */
  val fileSystemScheme = ""
  // val fileSystemScheme = "s3a://sages-aws"
  val useAws = false

  /**
    * Directory path with sample data
    */
  val dataPath: String = fileSystemScheme + System.getenv("HADOOP_DATA")

  /**
    * Wolne Lektury dataset
    */
  val bookPath: String = dataPath + "/dane/lektury/lektury-all"

  /**
    * Movielens dataset
    */
  val moviesPath: String = dataPath + "/dane/movielens/hive/movies/movies.dat"
  val tagsPath: String = dataPath + "/dane/movielens/hive/tags/tags.dat"
  val ratingsPath: String = dataPath + "/dane/movielens/hive/ratings/ratings.dat"
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

  /**
    * Output path (results)
    */
  val resultPath: String = "/user/" + user + "/wyniki/spark"

  def createSparkContext: SparkContext = {

    val conf = new SparkConf().
      setMaster(master).
      setAppName(user + " " + this.getClass.getSimpleName)

    val sc = new SparkContext(conf)

    sc
  }

}
