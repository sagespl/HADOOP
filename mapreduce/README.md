
# Instrukcja uruchomienia projektu na klastrze Apache Hadoop

## Ustawiamy ścieżki

```
export HDFS_HOME=/user/hadoop
export HADOOP_DATA=/home/radek/ownCloud/Sages/dane
export HADOOP_PROJECT=/home/radek/Sages/repository/HADOOP
```

## Wrzucamy nasze dane na hdfs'a

```
hdfs dfs -mkdir -p $HDFS_HOME/dane

hdfs dfs -copyFromLocal $HADOOP_DATA/lektury $HDFS_HOME/dane/
hdfs dfs -copyFromLocal $HADOOP_DATA/lektury-100/ $HDFS_HOME/dane/
hdfs dfs -copyFromLocal $HADOOP_DATA/lektury-one-file/ $HDFS_HOME/dane/
```

## Uruchomienie projektu

```
hadoop jar $HADOOP_PROJECT/mapreduce/target/mapreduce-1.0-SNAPSHOT-job.jar pl.com.sages.hadoop.mapreduce.wordcount.WordCountRunner $HDFS_HOME/dane/lektury $HDFS_HOME/wyniki/lektury-wordcount-output
hadoop jar $HADOOP_PROJECT/mapreduce/target/mapreduce-1.0-SNAPSHOT-job.jar pl.com.sages.hadoop.mapreduce.wordcount.WordCountRunner $HDFS_HOME/dane/lektury-100 $HDFS_HOME/wyniki/lektury-100-wordcount-output
hadoop jar $HADOOP_PROJECT/mapreduce/target/mapreduce-1.0-SNAPSHOT-job.jar pl.com.sages.hadoop.mapreduce.wordcount.WordCountRunner $HDFS_HOME/dane/lektury-one-file $HDFS_HOME/wyniki/lektury-one-file-wordcount-output

hadoop jar $HADOOP_PROJECT/mapreduce/target/mapreduce-1.0-SNAPSHOT-job.jar pl.com.sages.hadoop.mapreduce.invertedindex.InvertedIndexRunner $HDFS_HOME/dane/lektury-100 $HDFS_HOME/wyniki/lektury-100-invertedindex-output
hadoop jar $HADOOP_PROJECT/mapreduce/target/mapreduce-1.0-SNAPSHOT-job.jar pl.com.sages.hadoop.mapreduce.invertedindex.InvertedIndexRunner $HDFS_HOME/dane/lektury-one-file $HDFS_HOME/wyniki/lektury-one-file-invertedindex-output
```

## Jesli chcemy usunąć to co wcześniej wrzuciliśmy (dane plus wyniki)

```
hdfs dfs -rm -f -r -skipTrash $HDFS_HOME/dane
hdfs dfs -rm -f -r -skipTrash $HDFS_HOME/wyniki
```