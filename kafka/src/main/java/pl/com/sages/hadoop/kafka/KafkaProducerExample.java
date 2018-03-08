package pl.com.sages.hadoop.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import static pl.com.sages.hadoop.kafka.KafkaConfigurationFactory.SLEEP;
import static pl.com.sages.hadoop.kafka.KafkaConfigurationFactory.TOPIC;
import static pl.com.sages.hadoop.kafka.KafkaConfigurationFactory.createProducerConfig;

public class KafkaProducerExample {

    private static final Logger LOGGER = Logger.getLogger(KafkaProducerExample.class);

    public static void main(String[] args) throws Exception {

        Producer<String, String> producer = new KafkaProducer<>(createProducerConfig());
        LoggerCallback callback = new LoggerCallback();

        int messageId = 1;

        while (true) {

            for (long i = 0; i < 10; i++) {
                ProducerRecord<String, String> data = new ProducerRecord<>(TOPIC, "key-" + messageId, "message-" + messageId);
                producer.send(data, callback);
                messageId++;
            }


            LOGGER.info("Sended messages");
            Thread.sleep(SLEEP);
        }

//        producer.flush();
//        producer.close();
    }


}
