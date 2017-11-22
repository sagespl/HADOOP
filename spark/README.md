
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