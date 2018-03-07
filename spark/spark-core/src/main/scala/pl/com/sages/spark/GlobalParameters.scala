package pl.com.sages.spark

trait GlobalParameters {

  val user: String = System.getProperty("user.name")
  val master: String = "local"

  // books
  val bookPath: String = "/dane/lektury/lektury-all"

  // movielens
  val moviesPath: String = "/dane/movielens/hive/movies/movies.dat"
  val tagsPath: String = "/dane/movielens/hive/tags/tags.dat"
  val ratingsPath: String = "/dane/movielens/hive/ratings/ratings.dat"
  val movielensSeparator: String = "@"

  // Spark test data
  val sparkSampleData: String = System.getenv("HADOOP_DATA") + "/spark-data"
  val sparkSampleLibsvmData: String = sparkSampleData + "/mllib/sample_libsvm_data.txt"
  val sparkSampleMovielensRatingsData: String = sparkSampleData + "/mllib/als/sample_movielens_ratings.txt"
  val sparkAlsTestData: String = sparkSampleData + "/mllib/als/test.data"
  val sparkKMeansData: String = sparkSampleData + "/mllib/kmeans_data.txt"
  val sparkSampleKMeansData: String = sparkSampleData + "/mllib/sample_kmeans_data.txt"
  val sparkSampleLinearRegressionData: String = sparkSampleData + "/mllib/sample_linear_regression_data.txt"

  // results
  val resultPath: String = "/user/" + user + "/wyniki/spark"

}
