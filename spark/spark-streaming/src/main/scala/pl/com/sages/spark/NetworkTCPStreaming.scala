package pl.com.sages.spark

import org.apache.spark.storage.StorageLevel

/**
  * Run: nc -lk 9999
  */
object NetworkTCPStreaming extends BaseSparkStreamingApp with GlobalParameters {

  def main(args: Array[String]) {

    val ssc = createStreamingContext

    val lines = ssc.socketTextStream(hostname, port.toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    lines.foreachRDD { rdd => if (!rdd.isEmpty) println("hello world") }
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}