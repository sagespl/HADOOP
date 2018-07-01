package pl.com.sages.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class KafkaConfigurationFactory {

    public static final String KAFKA_SERVER = "cluster_kafka1:9092,cluster_kafka2:9092,cluster_kafka3:9092";
//     public static final String KAFKA_SERVER = "localhost:9092";
//     public static final String KAFKA_SERVER = "localhost:6667";

    public static final String ZOOKEEPER_SERVER = "cluster_zookeeper:2181";
//     public static final String ZOOKEEPER_SERVER = "localhost:2181";

    public static final String TOPIC = "test-topic";
    public static final String TOPIC2 = "test-topic2";
    public static final String TOPIC_OUT = "test-topic-out";

    public static final int TIMEOUT = 10000;
    public static final int SLEEP = 5000;

    /**
     * https://kafka.apache.org/documentation/#newconsumerconfigs
     */
    public static Properties createConsumerConfig() {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");

        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true); // if true manual commit is no needed

        return consumerConfig;
    }

    /**
     * https://kafka.apache.org/documentation/#producerconfigs
     */
    public static Properties createProducerConfig() {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        // producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "");

        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        // producerConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        // producerConfig.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        // producerConfig.put(ProducerConfig.LINGER_MS_CONFIG, "1");

        return producerConfig;
    }

    public static Properties getStreamConfig() {
        Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount");

        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return config;
    }

}
