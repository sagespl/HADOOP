package pl.com.sages.spark

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Run: nc -lk 9999
  */
object NetworkWordCount extends GlobalParameters {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(master).setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(conf, Seconds(1))

    val lines = ssc.socketTextStream(hostname, port.toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    lines.foreachRDD { rdd => if (!rdd.isEmpty) println("hello world") }
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}