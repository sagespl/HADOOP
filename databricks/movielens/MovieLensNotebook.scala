// Databricks notebook source
// List files on Databricks store (Azure blob)
display(dbutils.fs.ls("/FileStore/tables/movielens/hive"))

// COMMAND ----------

val directoryPath: String = "/FileStore/tables/movielens/hive"
val moviesPath: String = directoryPath + "/movies/movies.dat"
val ratingsPath: String = directoryPath + "/ratings/ratings.dat"
val tagsPath: String = directoryPath + "/tags/tags.dat"
val movielensSeparator: String = "@"

// reading from Data Lake
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

// COMMAND ----------

    moviesDataFrame.show(10)
    moviesDataFrame.printSchema()

// COMMAND ----------

    ratingsDataFrame.show(10)
    ratingsDataFrame.printSchema()

// COMMAND ----------

    tagsDataFrame.show(10)
    tagsDataFrame.printSchema()

// COMMAND ----------

    moviesDataFrame.createOrReplaceTempView("movies")
    ratingsDataFrame.createOrReplaceTempView("ratings")
    ratingsDataFrame.createOrReplaceTempView("tags")

// COMMAND ----------

val moviesAvg = spark.sql(
      """
      SELECT m.title, count(*) as votes, avg(rating) rate
      FROM movies m
      left join ratings r on m.movieid = r.movieid
      group by m.title
      having votes > 100
      order by rate desc
      """)

// COMMAND ----------

display(moviesAvg)

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT genre, count(1) as count 
// MAGIC from (
// MAGIC   select explode(split(genres, '\\|')) as genre
// MAGIC   from movies
// MAGIC )
// MAGIC group by genre
// MAGIC order by count desc
