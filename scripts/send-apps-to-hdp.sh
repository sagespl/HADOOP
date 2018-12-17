#!/bin/sh

scp -r $HADOOP_PROJECT/mapreduce/target/mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar radek@hdp1://home/radek

scp -r $HADOOP_PROJECT/hbase/hbase-mapred/target/hbase-mapred-1.0-SNAPSHOT-jar-with-dependencies.jar radek@hdp1://home/radek

scp -r $HADOOP_PROJECT/spark/spark-core/target/spark-core-1.0-SNAPSHOT-jar-with-dependencies.jar radek@hdp1://home/radek
scp -r $HADOOP_PROJECT/spark/spark-sql/target/spark-sql-1.0-SNAPSHOT-jar-with-dependencies.jar radek@hdp1://home/radek