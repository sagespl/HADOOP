package pl.com.sages.kafka

import java.util.Collections

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.log4j.Logger
import pl.com.sages.kafka.KafkaConfigurationFactory.{TIMEOUT, TOPIC, createConsumerConfig}

object KafkaConsumerScalaExample {

  private val LOGGER = Logger.getLogger(classOf[KafkaConsumerExample])

  def main(args: Array[String]): Unit = {

    import scala.collection.JavaConversions._

    val consumer = new KafkaConsumer[String, String](createConsumerConfig)
    consumer.subscribe(Collections.singletonList(TOPIC), new ConsumerRebalanceLoggerListener)

    try
        while (true) {

          val records = consumer.poll(TIMEOUT)
          if (records.count > 0) {
            LOGGER.info("Poll records: " + records.count)
            for (record <- records) {
              printf("Received Message topic = %s, partition = %s, offset = %d, key = %s, value = %s\n", record.topic, record.partition, record.offset, record.key, record.value)
            }
          }
          consumer.commitAsync()
        }

    catch {
      case e: Exception => LOGGER.error("Błąd...", e)
    } finally {
      consumer.commitSync()
      consumer.close()
    }

  }

}
