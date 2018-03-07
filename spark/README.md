
# Uruchomienie powłoki Spark 2 - Scala (Java)

Lokalnie (domyślne parametry)
~~~
export SPARK_MAJOR_VERSION=2
spark-shell
~~~

Lokalnie (8 wątków)
~~~
export SPARK_MAJOR_VERSION=2
spark-shell --master local[8]
~~~

Zdalnie (YARN)
~~~
export SPARK_MAJOR_VERSION=2
spark-shell --master yarn
~~~

# Uruchomienie powłoki Spark 2 - Python

~~~
export SPARK_MAJOR_VERSION=2
pyspark
~~~

# Build i SCP
~~~
cd $HADOOP_PROJECT/spark
mvn clean install -DskipTests=true
scp -r $HADOOP_PROJECT/spark/spark-core/target/spark-core-1.0-SNAPSHOT-jar-with-dependencies.jar hsandbox://tmp
scp -r $HADOOP_PROJECT/spark/spark-mllib/target/spark-mllib-1.0-SNAPSHOT-jar-with-dependencies.jar hsandbox://tmp
scp -r $HADOOP_PROJECT/spark/spark-sql/target/spark-sql-1.0-SNAPSHOT-jar-with-dependencies.jar hsandbox://tmp
scp -r $HADOOP_PROJECT/spark/spark-streaming/target/spark-streaming-1.0-SNAPSHOT-jar-with-dependencies.jar hsandbox://tmp
~~~