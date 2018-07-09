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
hive -f $HADOOP_PROJECT/hive/samples/movielens/create_database.sql
hive -f $HADOOP_PROJECT/hive/samples/movielens/create_tables_internal.sql
hive -f $HADOOP_PROJECT/hive/samples/movielens/create_tables_external.sql
hive -f $HADOOP_PROJECT/hive/samples/movielens/queries.sql
hive -f $HADOOP_PROJECT/hive/samples/movielens/drop_database.sql


echo "Pig"


echo "Spark"

export SPARK_MAJOR_VERSION=2

spark-submit --master yarn --deploy-mode cluster --num-executors 3 --class pl.com.sages.spark.core.MovieGenres $HADOOP_PROJECT/spark/spark-core/target/spark-core-1.0-SNAPSHOT-jar-with-dependencies.jar



echo "Kafka"


echo "Oozie"

hdfs dfs -rm -f -r -skipTrash $HADOOP_HDFS_HOME/oozie
hdfs dfs -mkdir -p $HADOOP_HDFS_HOME/oozie
hdfs dfs -mkdir -p $HADOOP_HDFS_HOME/oozie/lib

hdfs dfs -copyFromLocal $HADOOP_PROJECT/mapreduce/target/mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar $HADOOP_HDFS_HOME/oozie/lib/
hdfs dfs -copyFromLocal -f $HADOOP_PROJECT/oozie/src/main/resources/hive-script.hql $HADOOP_HDFS_HOME/oozie/
hdfs dfs -copyFromLocal -f $HADOOP_PROJECT/oozie/src/main/resources/workflow.xml $HADOOP_HDFS_HOME/oozie/

export OOZIE_URL=http://localhost:11000/oozie
oozie job -config $HADOOP_PROJECT/oozie/src/main/resources/job.properties -run