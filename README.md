# HADOOP
Materiały do szkolenia: HADOOP Projektowanie rozwiązań Big Data z wykorzystaniem Apache Hadoop &amp; Family

# Ustawienie zmiennych systemowych

* HADOOP_HDFS_HOME - katalog użytkownika na HDFS'ie
* HADOOP_HDFS_USER - nazwa użytkownika na HDFS'ie
* HADOOP_DATA - lokalizacja danych testowych (ścieżka lokalna)
* HADOOP_PROJECT - lokalizacja projektu HADOOP (ścieżka lokalna)

Przykład:

```
export HADOOP_HDFS_HOME=/user/sages
export HADOOP_HDFS_USER=sages
export HADOOP_DATA=/home/sages/dane
export HADOOP_PROJECT=/home/sages/repository/HADOOP
```

# Przykładowe zbiory testowe

MovieLens Dataset: https://grouplens.org/datasets/movielens/

```
cd $HADOOP_DATA

wget http://files.grouplens.org/datasets/movielens/ml-latest.zip
wget http://files.grouplens.org/datasets/movielens/ml-20m.zip
wget http://files.grouplens.org/datasets/movielens/ml-10m.zip

unzip ml-latest.zip
unzip ml-20m.zip
unzip ml-10m.zip
```

# Załadowanie na HDFS'a
~~~
hdfs dfs -copyFromLocal $HADOOP_DATA/ml-10M100K $HADOOP_HDFS_HOME/dane/
hdfs dfs -copyFromLocal $HADOOP_DATA/ml-20m $HADOOP_HDFS_HOME/dane/
hdfs dfs -copyFromLocal $HADOOP_DATA/ml-latest $HADOOP_HDFS_HOME/dane/

hdfs dfs -du -h $HADOOP_HDFS_HOME/dane
~~~

# Ambari

http://sandbox.hortonworks.com:8080

# Adresy i porty

https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.6.2/bk_reference/content/reference_chap2.html
https://hortonworks.com/tutorial/sandbox-port-forwarding-guide/section/1/

~~~
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "SSH"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "Ambari"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "NameNodeHttp"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "NameNodeHttps"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "NameNodeMetadata"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "DataNodeHttp"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "DataNodeHttps"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "DataNodeData"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "DataNodeDatas"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "DataNodeMetadata"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "SecondaryNameNodeHttp"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "ResourceManagerUI"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "JobHistoryUI"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "SparkHistoryUI"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "Spark2HistoryUI"

VBoxManage showvminfo "Hortonworks Data Platform Sandbox 2.6" | grep Rule

VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "SSH,tcp,127.0.0.1,2222,,22"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "Ambari,tcp,127.0.0.1,8080,,8080"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "NameNodeHttp,tcp,127.0.0.1,50070,,50070"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "NameNodeHttps,tcp,127.0.0.1,50470,,50470"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "NameNodeMetadata,tcp,127.0.0.1,8020,,8020"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "DataNodeHttp,tcp,127.0.0.1,50075,,50075"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "DataNodeHttps,tcp,127.0.0.1,50475,,50475"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "DataNodeData,tcp,127.0.0.1,50010,,50010"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "DataNodeDatas,tcp,127.0.0.1,1019,,1019"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "DataNodeMetadata,tcp,127.0.0.1,50020,,50020"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "SecondaryNameNodeHttp,tcp,127.0.0.1,50090,,50090"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "ResourceManagerUI,tcp,127.0.0.1,8088,,8088"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "JobHistoryUI,tcp,127.0.0.1,19888,,19888"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "SparkHistoryUI,tcp,127.0.0.1,18080,,18080"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "Spark2HistoryUI,tcp,127.0.0.1,18081,,18081"

VBoxManage showvminfo "Hortonworks Data Platform Sandbox 2.6" | grep Rule
~~~

~~~
VBoxManage startvm "Hortonworks Data Platform Sandbox 2.6"
VBoxManage startvm "Hortonworks Data Platform Sandbox 2.6" --type headless
~~~

# SSH

~~~
ssh sages@localhost -p 2222
ssh -L 50070:localhost:50070 sages@localhost -L 8020:localhost:8020 -p 2222
ssh -L 9092:localhost:9092 sages@localhost -L 9093:localhost:9093 -L 9094:localhost:9094 -p 2222
~~~