package pl.com.sages.spark.core

object WordCount extends BaseSparkApp {

  def main(args: Array[String]): Unit = {

    val sc = createSparkContext

    // run
    val booksRdd = sc.textFile(bookPath).coalesce(10)

    val wordsRdd = booksRdd.flatMap(_.split(" "))
    val wordCount = wordsRdd.map(x => (x, 1)).reduceByKey((x, y) => x + y)
    val sortedWordCount = wordCount.sortBy(p => p._2, ascending = false)

    // delete result directory and save result on HDFS
    import org.apache.hadoop.fs.{FileSystem, Path}
    FileSystem.get(sc.hadoopConfiguration).delete(new Path(resultPath), true)
    sortedWordCount.coalesce(1).saveAsTextFile(resultPath)

    // end
    sc.stop()
  }

}
