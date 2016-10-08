
# Instrukcja uruchomienia projektu na klastrze Apache Hadoop

## Wrzucamy nasze dane na hdfs'a

```
hdfs dfs -mkdir -p $HADOOP_HDFS_HOME/dane

hdfs dfs -copyFromLocal $HADOOP_DATA/lektury $HADOOP_HDFS_HOME/dane/
hdfs dfs -copyFromLocal $HADOOP_DATA/lektury-100/ $HADOOP_HDFS_HOME/dane/
hdfs dfs -copyFromLocal $HADOOP_DATA/lektury-one-file/ $HADOOP_HDFS_HOME/dane/
```

## Uruchomienie projektu

```
hadoop jar $HADOOP_PROJECT/mapreduce/target/mapreduce-1.0-SNAPSHOT-job.jar pl.com.sages.hadoop.mapreduce.wordcount.WordCountRunner $HADOOP_HDFS_HOME/dane/lektury $HADOOP_HDFS_HOME/wyniki/lektury-wordcount-output
hadoop jar $HADOOP_PROJECT/mapreduce/target/mapreduce-1.0-SNAPSHOT-job.jar pl.com.sages.hadoop.mapreduce.wordcount.WordCountRunner $HADOOP_HDFS_HOME/dane/lektury-100 $HADOOP_HDFS_HOME/wyniki/lektury-100-wordcount-output
hadoop jar $HADOOP_PROJECT/mapreduce/target/mapreduce-1.0-SNAPSHOT-job.jar pl.com.sages.hadoop.mapreduce.wordcount.WordCountRunner $HADOOP_HDFS_HOME/dane/lektury-one-file $HADOOP_HDFS_HOME/wyniki/lektury-one-file-wordcount-output

hadoop jar $HADOOP_PROJECT/mapreduce/target/mapreduce-1.0-SNAPSHOT-job.jar pl.com.sages.hadoop.mapreduce.invertedindex.InvertedIndexRunner $HADOOP_HDFS_HOME/dane/lektury-100 $HADOOP_HDFS_HOME/wyniki/lektury-100-invertedindex-output
hadoop jar $HADOOP_PROJECT/mapreduce/target/mapreduce-1.0-SNAPSHOT-job.jar pl.com.sages.hadoop.mapreduce.invertedindex.InvertedIndexRunner $HADOOP_HDFS_HOME/dane/lektury-one-file $HADOOP_HDFS_HOME/wyniki/lektury-one-file-invertedindex-output
```

## Jesli chcemy usunąć to co wcześniej wrzuciliśmy (dane plus wyniki)

```
hdfs dfs -rm -f -r -skipTrash $HADOOP_HDFS_HOME/dane
hdfs dfs -rm -f -r -skipTrash $HADOOP_HDFS_HOME/wyniki
```