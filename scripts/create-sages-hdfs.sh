#!/bin/sh

export HADOOP_USER_NAME=hdfs

hdfs dfs -rm -r -skipTrash /user/sages

hdfs dfs -mkdir /user/sages
hdfs dfs -chown sages /user/sages

hdfs dfs -ls /user

unset HADOOP_USER_NAME
