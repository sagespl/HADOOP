package pl.com.sages.hadoop.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

object KafkaProducerScalaExample {

  def main(args: Array[String]): Unit = {

    val props: Properties = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    while (true) {

      val producer = new KafkaProducer[String, String](props)

      for (i <- 1 to 10) {

        val data: ProducerRecord[String, String] = new ProducerRecord[String, String]("my-partitioned-topic", "key-" + i, "message-" + i)
        producer.send(data, new TestCallback())

      }

      producer.flush()
      producer.close()

      println("Sended messages")

      Thread.sleep(5000)
    }

  }

}