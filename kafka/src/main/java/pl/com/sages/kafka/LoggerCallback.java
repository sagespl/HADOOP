package pl.com.sages.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

public class LoggerCallback implements Callback {

    private static final Logger LOGGER = Logger.getLogger(LoggerCallback.class);

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