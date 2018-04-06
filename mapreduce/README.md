
# Instrukcja uruchomienia projektu na klastrze Apache Hadoop

## Wrzucamy nasze dane na hdfs'a

```
hdfs dfs -mkdir -p $HADOOP_HDFS_HOME/dane

hdfs dfs -copyFromLocal $HADOOP_DATA/lektury/lektury-all $HADOOP_HDFS_HOME/dane/
hdfs dfs -copyFromLocal $HADOOP_DATA/lektury/lektury-100/ $HADOOP_HDFS_HOME/dane/
hdfs dfs -copyFromLocal $HADOOP_DATA/lektury/lektury-one-file/ $HADOOP_HDFS_HOME/dane/
```

## Uruchomienie projektu

```
yarn jar $HADOOP_PROJECT/mapreduce/target/mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar pl.com.sages.hadoop.mapreduce.wordcount.WordCountRunner $HADOOP_HDFS_HOME/dane/lektury-all $HADOOP_HDFS_HOME/wyniki/lektury-wordcount-output
yarn jar $HADOOP_PROJECT/mapreduce/target/mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar pl.com.sages.hadoop.mapreduce.wordcount.WordCountRunner $HADOOP_HDFS_HOME/dane/lektury-100 $HADOOP_HDFS_HOME/wyniki/lektury-100-wordcount-output
yarn jar $HADOOP_PROJECT/mapreduce/target/mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar pl.com.sages.hadoop.mapreduce.wordcount.WordCountRunner $HADOOP_HDFS_HOME/dane/lektury-one-file $HADOOP_HDFS_HOME/wyniki/lektury-one-file-wordcount-output

yarn jar $HADOOP_PROJECT/mapreduce/target/mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar pl.com.sages.hadoop.mapreduce.invertedindex.InvertedIndexRunner $HADOOP_HDFS_HOME/dane/lektury-100 $HADOOP_HDFS_HOME/wyniki/lektury-100-invertedindex-output
yarn jar $HADOOP_PROJECT/mapreduce/target/mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar pl.com.sages.hadoop.mapreduce.invertedindex.InvertedIndexRunner $HADOOP_HDFS_HOME/dane/lektury-one-file $HADOOP_HDFS_HOME/wyniki/lektury-one-file-invertedindex-output
```

## Jesli chcemy usunąć to co wcześniej wrzuciliśmy (dane plus wyniki)

```
hdfs dfs -rm -f -r -skipTrash $HADOOP_HDFS_HOME/dane
hdfs dfs -rm -f -r -skipTrash $HADOOP_HDFS_HOME/wyniki
```


# Python MapReduce

## Przygotowanie danych
```
hdfs dfs -rm -f -r -skipTrash $HADOOP_HDFS_HOME/python
hdfs dfs -put $HADOOP_PROJECT/mapreduce/src/main/python $HADOOP_HDFS_HOME
```

## Testy

```
cat $HADOOP_DATA/lektury/lektury-all/aniol.txt | $HADOOP_PROJECT/mapreduce/src/main/python/map.py | sort \
 | $HADOOP_PROJECT/mapreduce/src/main/python/reduce.py | sort | grep -v 1
```

## Uruchomienie zadania

~~~
hdfs dfs -rm -f -r -skipTrash $HADOOP_HDFS_HOME/wyniki/lektury-one-file-wordcount-python-output

hadoop jar /usr/hdp/2.6.1.0-129/hadoop-mapreduce/hadoop-streaming.jar \
-file $HADOOP_PROJECT/mapreduce/src/main/python/map.py \
-mapper $HADOOP_PROJECT/mapreduce/src/main/python/map.py \
-file $HADOOP_PROJECT/mapreduce/src/main/python/reduce.py \
-reducer $HADOOP_PROJECT/mapreduce/src/main/python/reduce.py \
-input $HADOOP_HDFS_HOME/dane/lektury-one-file \
-output $HADOOP_HDFS_HOME/wyniki/lektury-one-file-wordcount-python-output
~~~

# YARN

https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/YarnCommands.html

~~~
yarn application -list

yarn application -kill application_1511465812466_0004

yarn logs -applicationId application_1511465812466_0004
~~~