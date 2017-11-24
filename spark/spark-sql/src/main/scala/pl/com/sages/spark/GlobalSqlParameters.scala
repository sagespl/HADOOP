package pl.com.sages.spark

trait GlobalSqlParameters {

  val bookPath: String = "/user/sages/dane/lektury-all"

  val moviesPath: String = "/user/sages/dane/movie/movies/movies.dat"
  val tagsPath: String = "/user/sages/dane/movie/tags/tags.dat"
  val ratingsPath: String = "/user/sages/dane/movie/ratings/ratings.dat"
  val movielensSeparator: String = "@"

  val resultPath: String = "/user/sages/wyniki/spark"

}
