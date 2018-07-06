package pl.com.sages.spark.core

object Movielens extends BaseSparkApp {

  def main(args: Array[String]): Unit = {

    val sc = createSparkContext

    // run
    val movies = sc.textFile(moviesPath)

    // Filmów jakiego gatunku jest najwięcej?
    val genres = movies.
      map(line => line.split(movielensSeparator)(2)).
      flatMap(genres => genres.split("\\|")).
      countByValue().
      toList.
      sortBy {
        -_._2
      }

    // w którym roku powstało najwięcej filmów?
    var regex =
      """\(([0-9]+)\)""".r
    var year = movies.
      map(line => (regex.findFirstMatchIn(line).orNull.group(1), 1)).
      reduceByKey((x, y) => x + y)
    year.takeOrdered(10)(Ordering[Int].reverse.on(x => x._2))

    // Jaki jest najlepszy film wszechczasów? Oblicz na podstawie średniej oceny użytkowników.
    var moviesRdd = sc.textFile(moviesPath).
      map(line => line.split(movielensSeparator)).
      map(line => (line(0), line(1)))

    var ratingsRdd = sc.textFile(ratingsPath).
      map(line => line.split(movielensSeparator)).
      map(line => (line(1), line(2).toDouble))

    var joined = moviesRdd.join(ratingsRdd)

    joined.cache()

    joined.map(value => value._2).
      mapValues(x => (x, 1)).
      reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).
      filter(x => x._2._2 > 100).
      mapValues(x => x._1 / x._2).
      sortBy {
        -_._2
      }.
      take(10)

    // end
    sc.stop()
  }

}
