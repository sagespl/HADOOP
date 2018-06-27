#!/bin/sh

docker exec -it cluster_zookeeper ip r | grep src

docker exec -it cluster_kafka1 ip r | grep src

docker exec -it cluster_kafka2 ip r | grep src

docker exec -it cluster_kafka3 ip r | grep src