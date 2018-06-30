package pl.com.sages.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import static pl.com.sages.kafka.KafkaConfigurationFactory.*;

public class KafkaProducerExample {

    private static final Logger LOGGER = Logger.getLogger(KafkaProducerExample.class);
    private static int MESSAGE_ID = 1;

    public static void main(String[] args) {

        Producer<String, String> producer = new KafkaProducer<>(createProducerConfig());
        LoggerCallback callback = new LoggerCallback();

        try {
            while (true) {

                for (long i = 0; i < 10; i++) {
                    ProducerRecord<String, String> data = new ProducerRecord<>(TOPIC, "key-" + MESSAGE_ID, " Ala ma kota, Ela ma psa");

                    producer.send(data, callback); // async with callback
                    // producer.send(data); // async without callback
                    // producer.send(data).get(); // sync send

                    MESSAGE_ID++;
                }

                LOGGER.info("Sended messages");
                Thread.sleep(SLEEP);
            }
        } catch (Exception e) {
            LOGGER.error("Błąd...", e);
        } finally {
            producer.flush();
            producer.close();
        }

    }

}
