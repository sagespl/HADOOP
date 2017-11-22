package pl.com.sages.spark

trait GlobalParameters {

  val moviesPath: String = "/user/sages/dane/ml-10M100K/movies.dat"
  val tagsPath: String = "/user/sages/dane/ml-10M100K/tags.dat"
  val ratingsPath: String = "/user/sages/dane/ml-10M100K/ratings.dat"
  val movielensSeparator: String = "::"
  val resultPath: String = "/user/sages/wyniki/spark"

}
