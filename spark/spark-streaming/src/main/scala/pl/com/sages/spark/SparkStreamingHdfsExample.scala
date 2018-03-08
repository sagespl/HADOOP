package pl.com.sages.spark


import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingHdfsExample extends GlobalParameters {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(master).setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(conf, Seconds(2))

    val lines = ssc.textFileStream(bookPath)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
