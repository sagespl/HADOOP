package pl.com.sages.spark

import org.apache.spark.sql.SparkSession

object MovieLensDataset extends GlobalSqlParameters {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName(this.getClass.getSimpleName).getOrCreate()
    import spark.implicits._

    // data class
    case class Movie(movieId: String, title: String, genres: String)
    val movieEncoder = Seq(Movie("", "", "")).toDS
    movieEncoder.show()

    // reading from HDFS
    val moviesDataset = spark.read.
      option("header", "false").
      option("charset", "UTF8").
      option("delimiter", movielensSeparator).
      option("inferSchema", "true").
      csv(moviesPath).
      withColumnRenamed("_c0", "movieId").
      withColumnRenamed("_c1", "title").
      withColumnRenamed("_c2", "genres").
      as[Movie]

    // show
    moviesDataset.show(10)

    // SQL ;)
    moviesDataset.printSchema()
    moviesDataset.select("title").show()
    moviesDataset.groupBy("genres").count().show()
    moviesDataset.filter($"title".contains("2005")).show()

    moviesDataset.createOrReplaceTempView("movies")
    spark.sql("SELECT * FROM movies").show()

    // transform
    moviesDataset.map(movie => "Movie: " + movie(1)).show()

    // aggregation
    val resultDF = moviesDataset.
      groupBy("movieId").
      avg("rating").
      as("r").
      join(moviesDataset.as("m"), $"m.movieId" === $"r.movieId")

    resultDF.show
    resultDF.printSchema()

    // end
    spark.stop()
  }

}