package pl.com.sages.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

trait BaseSparkStreamingApp extends GlobalParameters {

  def createStreamingContext: StreamingContext = {

    val conf = new SparkConf().
      setMaster(master).
      setAppName(user + " " + this.getClass.getSimpleName)

    val ssc = new StreamingContext(conf, Seconds(5))

    ssc
  }

}
