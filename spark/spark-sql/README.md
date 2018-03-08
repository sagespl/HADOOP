
# Uruchomienie jar'a

~~~
export SPARK_MAJOR_VERSION=2

spark-submit --master yarn --deploy-mode cluster --num-executors 3 --class pl.com.sages.spark.SparkDatasetBasic /tmp/spark-sql-1.0-SNAPSHOT-jar-with-dependencies.jar
spark-submit --master yarn --deploy-mode cluster --num-executors 3 --class pl.com.sages.spark.SparkDatasetQuickStart /tmp/spark-sql-1.0-SNAPSHOT-jar-with-dependencies.jar
spark-submit --master yarn --deploy-mode cluster --num-executors 3 --class pl.com.sages.spark.SparkDatasetSampleApp /tmp/spark-sql-1.0-SNAPSHOT-jar-with-dependencies.jar

spark-submit --master yarn --deploy-mode cluster --num-executors 3 --class pl.com.sages.spark.MovieLensDataset /tmp/spark-sql-1.0-SNAPSHOT-jar-with-dependencies.jar
spark-submit --master yarn --deploy-mode cluster --num-executors 3 --class pl.com.sages.spark.MovieLensDataFrame /tmp/spark-sql-1.0-SNAPSHOT-jar-with-dependencies.jar

hdfs dfs -ls $HADOOP_HDFS_HOME/wyniki/spark/
hdfs dfs -cat $HADOOP_HDFS_HOME/wyniki/spark/part-00000
~~~