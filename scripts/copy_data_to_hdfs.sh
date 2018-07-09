#!/bin/sh

export HADOOP_USER_NAME=hdfs

hdfs dfs -rm -r -skipTrash /dane

hdfs dfs -mkdir /dane
hdfs dfs -chmod 777 /dane
hdfs dfs -put $HADOOP_DATA/* /dane

hdfs dfs -ls /dane

unset HADOOP_USER_NAME
