package pl.com.sages.spark

import org.apache.spark.{SparkConf, SparkContext}

object MovieGenres extends GlobalParameters {

  def main(args: Array[String]): Unit = {

    // prepare
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    // run
    val movies = sc.textFile(moviesPath)

    //    movies.take(10).foreach(println)

    val result = sc.textFile(moviesPath).
      map(_.split(movielensSeparator)(2)).
      flatMap(_.split("\\|")).
      map(genre => (genre, 1)).
      reduceByKey((x, y) => x + y).
      sortBy(-_._2)

    //    result.take(10).foreach(println)

    // delete result directory and save result on HDFS
    import org.apache.hadoop.fs.{FileSystem, Path}
    FileSystem.get(sc.hadoopConfiguration).delete(new Path(resultPath), true)
    result.saveAsTextFile(resultPath)

    // end
    sc.stop()
  }

}
