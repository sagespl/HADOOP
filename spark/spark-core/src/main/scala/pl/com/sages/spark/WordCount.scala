package pl.com.sages.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount extends GlobalParameters {

  def main(args: Array[String]): Unit = {

    // prepare
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

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
