package pl.com.sages.spark.streaming

import pl.com.sages.spark.core.GlobalParameters

object HdfsStreaming extends BaseSparkStreamingApp with GlobalParameters {

  def main(args: Array[String]) {

    val ssc = createStreamingContext

    val lines = ssc.textFileStream(bookPath)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
