
HADOOP_USER_NAME=hdfs

hdfs dfs -rm -r -skipTrash /dane

hdfs dfs -cp $HADOOP_DATA /

hdfs dfs -ls /dane

unset HADOOP_USER_NAME