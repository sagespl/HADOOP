
# Wysłanie jar'a na klaster

~~~
scp -r $HADOOP_PROJECT/hbase/hbase-spark/target/hbase-spark-1.0-SNAPSHOT-jar-with-dependencies.jar hsandbox://tmp
~~~


# Uruchomienie operacji Apache Spark na bazie HBase

~~~
export SPARK_MAJOR_VERSION=2

spark-submit --master yarn --deploy-mode cluster --num-executors 3 --class pl.com.sages.hbase.spark.SparkHBase /tmp/hbase-spark-1.0-SNAPSHOT-jar-with-dependencies.jar
~~~