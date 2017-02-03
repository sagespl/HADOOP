package pl.com.sages.hadoop.kafka;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import java.util.Collection;

public class TestConsumerRebalanceListener implements ConsumerRebalanceListener {

    private static final Logger LOGGER = Logger.getLogger(TestConsumerRebalanceListener.class);

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        LOGGER.info("Called onPartitionsRevoked with partitions:" + partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        LOGGER.info("Called onPartitionsAssigned with partitions:" + partitions);
    }

}