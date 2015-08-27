
WordCount

1. Wrzucamy nasze dane na hdfs'a

hdfs dfs -mkdir -p /user/sages/dane

hdfs dfs -copyFromLocal /home/sages/Sages/dane/lektury /user/sages/dane/
hdfs dfs -copyFromLocal /home/sages/Sages/dane/lektury-100/ /user/sages/dane/
hdfs dfs -copyFromLocal /home/sages/Sages/dane/lektury-one-file/ /user/sages/dane/

2. Uruchomienie projektu

#hadoop jar /home/sages/Sages/sages-hadoop/mapreduce/target/mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar wordcount /user/sages/dane/lektury /user/sages/wyniki/lektury-wordcount-output
hadoop jar /home/sages/Sages/sages-hadoop/mapreduce/target/mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar wordcount /user/sages/dane/lektury-100 /user/sages/wyniki/lektury-100-wordcount-output
hadoop jar /home/sages/Sages/sages-hadoop/mapreduce/target/mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar wordcount /user/sages/dane/lektury-one-file /user/sages/wyniki/lektury-one-file-wordcount-output

hadoop jar /home/sages/Sages/sages-hadoop/mapreduce/target/mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar invertedindex /user/sages/dane/lektury-100 /user/sages/wyniki/lektury-100-invertedindex-output
hadoop jar /home/sages/Sages/sages-hadoop/mapreduce/target/mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar invertedindex /user/sages/dane/lektury-one-file /user/sages/wyniki/lektury-one-file-invertedindex-output


3 Jesli chcemy usunąć to co wcześniej wrzuciliśmy (dane plus wyniki)

hdfs dfs -rm -f -r -skipTrash /user/sages/dane
hdfs dfs -rm -f -r -skipTrash /user/sages/wyniki