package pl.com.sages.spark.sql

import pl.com.sages.spark.core.BaseSparkApp

object MovieLensDataFrame extends BaseSparkSqlApp with BaseSparkApp {

  def main(args: Array[String]): Unit = {

    val spark = createSparkSession
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

    val ratingsDataFrame = spark.read.
      option("header", "false").
      option("charset", "UTF8").
      option("delimiter", movielensSeparator).
      option("inferSchema", "true").
      csv(ratingsPath).
      withColumnRenamed("_c0", "userId").
      withColumnRenamed("_c1", "movieId").
      withColumnRenamed("_c2", "rating").
      withColumnRenamed("_c3", "timestamp")

    val tagsDataFrame = spark.read.
      option("header", "false").
      option("charset", "UTF8").
      option("delimiter", movielensSeparator).
      option("inferSchema", "true").
      csv(tagsPath).
      withColumnRenamed("_c0", "userId").
      withColumnRenamed("_c1", "movieId").
      withColumnRenamed("_c2", "tag").
      withColumnRenamed("_c3", "timestamp")

    // persist
    moviesDataFrame.persist()
    ratingsDataFrame.persist()

    // show
    moviesDataFrame.show(10)
    moviesDataFrame.printSchema()

    ratingsDataFrame.show(10)
    ratingsDataFrame.printSchema()

    tagsDataFrame.show(10)
    tagsDataFrame.printSchema()

    // SQL ;)
    moviesDataFrame.select("title").show()
    moviesDataFrame.filter($"title".contains("2005")).show()

    ratingsDataFrame.select($"userid", $"movieid", $"rating" + 1).show()
    ratingsDataFrame.filter($"rating" < 2).show()
    ratingsDataFrame.groupBy("movieid").count().show()

    // view
    moviesDataFrame.createOrReplaceTempView("movies")
    ratingsDataFrame.createOrReplaceTempView("ratings")
    ratingsDataFrame.createOrReplaceTempView("tags")

    // queries
    spark.sql("SELECT * FROM movies").show()
    spark.sql("SELECT * FROM ratings").show()
    spark.sql("SELECT movieId, avg(rating) as rating FROM ratings group by movieId").show()
    spark.sql("SELECT movieId, count(*) as counted " +
      "FROM ratings " +
      "group by movieId " +
      "order by counted desc").show()
    spark.sql(
      """
      SELECT m.title, count(*) as votes, avg(rating) rate
      FROM movies m
      left join ratings r on m.movieid = r.movieid
      group by m.title
      having votes > 100
      order by rate desc
      """).show()

    // global view
    moviesDataFrame.createGlobalTempView("gmovies")
    spark.sql("SELECT * FROM global_temp.gmovies").show()

    // transform
    moviesDataFrame.map(movie => "Movie: " + movie(1)).show()

    // aggregation
    val resultDF = ratingsDataFrame.
      groupBy("movieId").
      avg("rating").
      as("r").
      join(moviesDataFrame.as("m"), $"m.movieId" === $"r.movieId")

    resultDF.show
    resultDF.printSchema()

    // end
    spark.stop()
  }

}