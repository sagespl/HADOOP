package pl.com.sages.hadoop.kafka

import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}

import scala.collection.JavaConversions._

object KafkaConsumerScalaExample {

  def main(args: Array[String]): Unit = {

    val consumerConfig: Properties = new Properties
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    //        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group" + new Random(System.currentTimeMillis()).nextInt());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group")
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](consumerConfig)
    consumer.subscribe(Collections.singletonList("my-partitioned-topic"), new ConsumerRebalanceLoggerListener)

    while (true) {
      val records: ConsumerRecords[String, String] = consumer.poll(10000)
      if (records.count > 0) {
        println("Poll records: " + records.count)

        for (record <- records) {
          printf("Received Message topic = %s, partition = %s, offset = %d, key = %s, value = %s\n", record.topic, record.partition, record.offset, record.key, record.value)
        }
      }
      consumer.commitSync()
    }

  }

}
