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

# Ambari

http://sandbox.hortonworks.com:8080

# Adresy i porty

VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "SSH"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "Ambari"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "NameNodeHttp"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "NameNodeHttps"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "NameNodeMetadata"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "DataNodeHttp"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "DataNodeHttps"

VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "SSH,tcp,,2222,,22"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "Ambari,tcp,,8080,,8080"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "NameNodeHttp,tcp,,50070,,50070"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "NameNodeHttps,tcp,,50470,,50470"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "NameNodeMetadata,tcp,,8020,,8020"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "DataNodeHttp,tcp,,50075,,50075"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "DataNodeHttps,tcp,,50475,,50475"