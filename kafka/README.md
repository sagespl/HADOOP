
### Docker
bash kontenera
~~~bash
docker exec -it cluster_kafka1 bash
cd /opt/kafka_2.12-1.1.0/
~~~


# Docker zmienne do poleceń w konsoli

~~~bash
export KAFKA_ZOOKEEPER=cluster_zookeeper:2181
export KAFKA_BROKER=cluster_kafka1:9092,cluster_kafka2:9092,cluster_kafka3:9092
export TOPIC=test-topic
export TOPIC_OUT=test-topic-out
~~~



### Stworzenie topicu
~~~bash
bin/kafka-topics.sh --create --zookeeper $KAFKA_ZOOKEEPER \
    --replication-factor 1 --partitions 1 --topic $TOPIC
~~~



### Informacje o topicu
~~~bash
bin/kafka-topics.sh --describe --zookeeper $KAFKA_ZOOKEEPER
bin/kafka-topics.sh --describe --zookeeper $KAFKA_ZOOKEEPER \
    --topic $TOPIC
bin/kafka-topics.sh --zookeeper $KAFKA_ZOOKEEPER --describe \
    --under-replicated-partitions
~~~



### Edycja topicu
~~~bash
bin/kafka-topics.sh --alter --zookeeper $KAFKA_ZOOKEEPER \
    --topic $TOPIC --partitions 3
~~~



### Lista topiców
~~~bash
bin/kafka-topics.sh --list --zookeeper $KAFKA_ZOOKEEPER
~~~



### Topic z replikami
~~~bash
bin/kafka-topics.sh --create --zookeeper $KAFKA_ZOOKEEPER \
    --replication-factor 3 --partitions 3 --topic my-super-topic
bin/kafka-topics.sh --describe --zookeeper $KAFKA_ZOOKEEPER \
    --topic my-super-topic
~~~




### Producent i konsument w konsoli

~~~bash
bin/kafka-console-producer.sh --broker-list $KAFKA_BROKER \
    --topic $TOPIC
~~~

~~~bash
bin/kafka-console-consumer.sh --bootstrap-server $KAFKA_BROKER \
    --topic $TOPIC --from-beginning

bin/kafka-console-consumer.sh --bootstrap-server $KAFKA_BROKER \
    --topic $TOPIC \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
~~~