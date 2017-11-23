package pl.com.sages.spark

import org.apache.spark.sql.SparkSession

object MovieLensDataset extends GlobalSqlParameters {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName(this.getClass.getSimpleName).getOrCreate()

    // Implicit methods available in Scala for converting common Scala objects into DataFrames/Datasets
    import spark.implicits._

    // reading from HDFS
    val moviesDataFrame = spark.read.
      option("header", "false").
      option("charset", "UTF8").
      option("delimiter", movielensSeparator).
      option("inferSchema", "true").
      csv(moviesPath).
      withColumnRenamed("_c0", "movieId").
      withColumnRenamed("_c1", "title").
      withColumnRenamed("_c2", "genres")

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
    moviesDataFrame.show(10)
    moviesDataset.show(10)

    // SQL ;)
    moviesDataFrame.printSchema()
    moviesDataFrame.select("title").show()
    moviesDataFrame.groupBy("genres").count().show()
    moviesDataFrame.filter($"title".contains("2005")).show()

    moviesDataFrame.createOrReplaceTempView("movies")
    spark.sql("SELECT * FROM movies").show()

    // transform
    moviesDataFrame.map(movie => "Movie: " + movie(1)).show()

    // end
    spark.stop()
  }

}