
# Wysłanie jar'a na klaster

~~~
scp -r $HADOOP_PROJECT/hbase/hbase-spark/target/hbase-spark-1.0-SNAPSHOT-jar-with-dependencies.jar hsandbox://tmp
scp -r $HADOOP_PROJECT/hbase/hbase-spark/target/hbase-spark-1.0-SNAPSHOT-jar-with-dependencies.jar hdp1://tmp
~~~


# Uruchomienie operacji Apache Spark na bazie HBase

~~~
export SPARK_MAJOR_VERSION=2

spark-submit --master yarn --deploy-mode cluster --num-executors 3 --class pl.com.sages.hbase.spark.SparkHbaseMoviesFilter /tmp/hbase-spark-1.0-SNAPSHOT-jar-with-dependencies.jar
spark-submit --master yarn --deploy-mode cluster --num-executors 3 --class pl.com.sages.hbase.spark.SparkHbaseMoviesGenres /tmp/hbase-spark-1.0-SNAPSHOT-jar-with-dependencies.jar
~~~