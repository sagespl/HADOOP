package pl.com.sages.spark.streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import pl.com.sages.spark.core.BaseSparkApp

/**
  * Run: nc -lk 9999
  */
object NetworkTCPStreaming extends BaseSparkStreamingApp with BaseSparkApp {

  def main(args: Array[String]) {

    val ssc = createStreamingContext

    val lines: DStream[String] = ssc.socketTextStream(hostname, port.toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    wordCounts.print()
    wordCounts.count().print()

    ssc.start()
    ssc.awaitTermination()
  }

}