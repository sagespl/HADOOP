
# Wrzucenie na serwer

~~~
scp -r $HADOOP_PROJECT/spark/spark-sql/target/spark-sql-1.0-SNAPSHOT-job.jar sandbox://tmp
~~~

# Uruchomienie jar'a

~~~
export SPARK_MAJOR_VERSION=2
spark-submit --master yarn --deploy-mode cluster --num-executors 3 --class pl.com.sages.spark.SparkQuickStartApp /tmp/spark-sql-1.0-SNAPSHOT-job.jar
~~~