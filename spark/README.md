
# Uruchomienie powłoki Spark 2

~~~
export SPARK_MAJOR_VERSION=2
spark-shell
~~~

# Uruchomienie jar'a

~~~
export SPARK_MAJOR_VERSION=2
spark-submit --master yarn --deploy-mode cluster --num-executors 7 --class pl.com.sages.spark.SparkQuickStartApp /tmp/spark-1.0-SNAPSHOT-job.jar
~~~


# Wrzucenie na serwer

~~~
scp -r $HADOOP_PROJECT/spark/target/spark-1.0-SNAPSHOT-job.jar sandbox://tmp
~~~