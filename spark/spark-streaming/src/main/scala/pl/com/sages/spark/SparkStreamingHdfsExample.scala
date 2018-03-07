package pl.com.sages.spark


import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingHdfsExample extends GlobalParameters {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("HdfsWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val lines = ssc.textFileStream(bookPath)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
