
# Apache Kafka

cd kafka_2.11-1.0.0/

### Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

### Kafka
bin/kafka-server-start.sh config/server.properties

### topic create
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

### topic list
bin/kafka-topics.sh --list --zookeeper localhost:2181

### send message
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test

### receive messages
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning
