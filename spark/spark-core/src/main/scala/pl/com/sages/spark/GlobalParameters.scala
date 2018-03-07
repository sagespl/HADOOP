package pl.com.sages.spark

trait GlobalParameters {

  val user: String = "sages"

  // books
  val bookPath: String = "/dane/lektury/lektury-all"

  // movielens
  val moviesPath: String = "/dane/movielens/hive/movies/movies.dat"
  val tagsPath: String = "/dane/movielens/hive/tags/tags.dat"
  val ratingsPath: String = "/dane/movielens/hive/ratings/ratings.dat"
  val movielensSeparator: String = "@"

  // results
  val resultPath: String = "/user/" + user + "/wyniki/spark"

}
