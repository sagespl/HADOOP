
export HADOOP_USER_NAME=hdfs

hdfs dfs -rm -r -skipTrash /dane

hdfs dfs -mkdir /dane
hdfs dfs -cp $HADOOP_DATA/* /dane

hdfs dfs -ls /dane

unset HADOOP_USER_NAME