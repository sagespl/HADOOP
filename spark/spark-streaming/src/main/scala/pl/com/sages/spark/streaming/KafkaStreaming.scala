package pl.com.sages.spark.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import pl.com.sages.spark.core.BaseSparkApp

object KafkaStreaming extends BaseSparkStreamingApp with BaseSparkApp {

  def main(args: Array[String]) {

    val ssc = createStreamingContext

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaBootstrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> kafkaGroupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(kafkaTopics)

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_.value)
    lines.print()
    lines.count().print()

    val words = lines.flatMap(_.split(" "))
    words.print()
    words.count().print()

    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)

    wordCounts.print()
    wordCounts.count().print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
