
# Docker zmienne do poleceń w konsoli

~~~bash
export KAFKA_ZOOKEEPER=cluster_zookeeper:2181
export KAFKA_BROKER=cluster_kafka1:9092,cluster_kafka2:9092,cluster_kafka3:9092
export TOPIC=test-topic
~~~


# Apache Kafka

cd kafka_2.11-1.0.0/
cd /usr/hdp/2.6.2.0-205/kafka/
cd /usr/hdp/current/kafka-broker/

### Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

### Kafka
bin/kafka-server-start.sh config/server.properties




### topic create
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic test-topic

### topic list
bin/kafka-topics.sh --list --zookeeper localhost:2181

### delete topic (delete.topic.enable)
bin/kafka-topics.sh --delete  --zookeeper localhost:2181  --topic test-topic





### send message
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test-topic

### receive messages
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning

bin/kafka-console-consumer.sh --bootstrap-server localhost:6667 \
    --topic test-topic-out \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
