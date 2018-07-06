package pl.com.sages.spark.sql

import pl.com.sages.spark.core.BaseSparkApp
import pl.com.sages.spark.sql.model.{Movie, Rating}

object MovieLensDataset extends BaseSparkSqlApp with BaseSparkApp {

  def main(args: Array[String]): Unit = {

    val spark = createSparkSession
    import spark.implicits._

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

    val ratingsDataset = spark.read.
      option("header", "false").
      option("charset", "UTF8").
      option("delimiter", movielensSeparator).
      option("inferSchema", "true").
      csv(ratingsPath).
      withColumnRenamed("_c0", "userId").
      withColumnRenamed("_c1", "movieId").
      withColumnRenamed("_c2", "rating").
      withColumnRenamed("_c3", "timestamp").
      as[Rating]

    // show
    moviesDataset.show(10)
    moviesDataset.printSchema()

    ratingsDataset.show(10)
    ratingsDataset.printSchema()

    // SQL ;)
    moviesDataset.select("title").show()
    moviesDataset.filter($"title".contains("2005")).show()

    ratingsDataset.select($"userid",$"movieid", $"rating" + 1).show()
    ratingsDataset.filter($"rating" < 2).show()
    ratingsDataset.groupBy("movieid").count().show()

    // view
    moviesDataset.createOrReplaceTempView("movies")
    spark.sql("SELECT * FROM movies").show()

    // transform
    moviesDataset.map(movie => "Movie: " + movie.title).show()

    // aggregation
    val resultDF = ratingsDataset.
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