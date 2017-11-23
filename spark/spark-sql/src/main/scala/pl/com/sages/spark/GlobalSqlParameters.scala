package pl.com.sages.spark

trait GlobalSqlParameters {

  val bookPath: String = "/user/sages/dane/lektury-all"

  val moviesPath: String = "/user/sages/dane/movie/movies/movies.dat"
  val tagsPath: String = "/user/sages/dane/ml-10M100K/tags.dat"
  val ratingsPath: String = "/user/sages/dane/ml-10M100K/ratings.dat"
  val movielensSeparator: String = "@"

  val resultPath: String = "/user/sages/wyniki/spark"

}
