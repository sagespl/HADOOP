package pl.com.sages.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import pl.com.sages.spark.core.GlobalParameters

trait BaseSparkStreamingApp extends GlobalParameters {

  def createStreamingContext: StreamingContext = {

    val conf = new SparkConf().
      setMaster(master).
      setAppName(user + " " + this.getClass.getSimpleName)

    val ssc = new StreamingContext(conf, Seconds(10))

    ssc
  }

}
