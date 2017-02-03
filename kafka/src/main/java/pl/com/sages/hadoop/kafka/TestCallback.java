package pl.com.sages.hadoop.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

public class TestCallback implements Callback {

    private static final Logger LOGGER = Logger.getLogger(TestCallback.class);

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
            LOGGER.error("Error while producing message to topic :" + recordMetadata);
            e.printStackTrace();
        } else {
            String message = String.format("sent message to topic:%s partition:%s  offset:%s", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
            LOGGER.info(message);
        }
    }

}