package pl.com.sages.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import java.util.Collections;

import static pl.com.sages.kafka.KafkaConfigurationFactory.*;

/**
 * The consumer is designed to be run in its own thread!!!
 */
public class KafkaConsumerExample {

    private static final Logger LOGGER = Logger.getLogger(KafkaConsumerExample.class);

    public static void main(String[] args) {

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(createConsumerConfig());

        consumer.subscribe(Collections.singletonList(TOPIC), new ConsumerRebalanceLoggerListener());
        // consumer.subscribe(Arrays.asList(TOPIC, TOPIC2), new ConsumerRebalanceLoggerListener());

        try {
            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(TIMEOUT);
                if (records.count() > 0) {
                    LOGGER.info("Poll records: " + records.count());

                    for (ConsumerRecord<String, String> record : records) {
                        System.out.printf("Received Message topic = %s, partition = %s, offset = %d, key = %s, value = %s\n",
                                record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    }
                }

                consumer.commitAsync(); // async commit (+ callback)
            }
        } catch (Exception e) {
            LOGGER.error("Błąd...", e);
        } finally {
            consumer.commitSync(); // sync commit
            consumer.close();
        }
    }

}
