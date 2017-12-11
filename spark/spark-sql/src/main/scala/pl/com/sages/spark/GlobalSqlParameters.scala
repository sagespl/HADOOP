package pl.com.sages.spark

trait GlobalSqlParameters {

  val bookPath: String = "/user/sages/dane/lektury/lektury-all"

  val moviesPath: String = "/user/sages/dane/movielens/hive/movies/movies.dat"
  val tagsPath: String = "/user/sages/dane/movielens/hive/tags/tags.dat"
  val ratingsPath: String = "/user/sages/dane/movielens/hive/ratings/ratings.dat"
  val movielensSeparator: String = "@"

  val resultPath: String = "/user/sages/wyniki/spark"

}
