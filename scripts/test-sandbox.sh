#!/usr/bin/env bash

hdfs dfs -ls /user

echo "Maven"
cd /home/sages/repository/HADOOP
mvn clean install -Puber,sandbox


echo "MapReduce"
hdfs dfs -rm -f -r -skipTrash $HADOOP_HDFS_HOME/dane
hdfs dfs -rm -f -r -skipTrash $HADOOP_HDFS_HOME/wyniki

yarn jar $HADOOP_PROJECT/mapreduce/target/mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar pl.com.sages.hadoop.mapreduce.wordcount.WordCountRunner /dane/lektury/lektury-one-file $HADOOP_HDFS_HOME/wyniki/lektury-one-file-wordcount-output
yarn jar $HADOOP_PROJECT/mapreduce/target/mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar pl.com.sages.hadoop.mapreduce.invertedindex.InvertedIndexRunner /dane/lektury/lektury-one-file $HADOOP_HDFS_HOME/wyniki/lektury-one-file-invertedindex-output


echo "Hive"


echo "Pig"


echo "Spark"