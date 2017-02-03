package pl.com.sages.hadoop.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.Properties;

public class KafkaProducerExample {

    private static final Logger LOGGER = Logger.getLogger(KafkaProducerExample.class);

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        while (true) {

            Producer<String, String> producer = new KafkaProducer<>(props);
            TestCallback callback = new TestCallback();

            for (long i = 0; i < 10; i++) {
                ProducerRecord<String, String> data = new ProducerRecord<>("test", "key-" + i, "message-" + i);
                producer.send(data, callback);
            }

            producer.flush();
            producer.close();

            LOGGER.info("Sended messages");
            Thread.sleep(5000);
        }

    }

}
