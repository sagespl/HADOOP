
hdfs dfs -rm -r -skipTrash /dane

hdfs dfs -mkdir /dane
hdfs dfs -cp $HADOOP_DATA/* /dane

hdfs dfs -ls /dane

