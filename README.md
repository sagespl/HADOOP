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

VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "ssh"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "ambari"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "namenode"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "datanode"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "jobtracker"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "tasktracker"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "hbase"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "allapps"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 delete "hue"

VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "ssh,tcp,,2222,,22"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "ambari,tcp,,8080,,8080"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "namenode,tcp,,50070,,50070"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "datanode,tcp,,50075,,50075"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "jobtracker,tcp,,50030,,50030"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "tasktracker,tcp,,50060,,50060"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "hbase,tcp,,60010,,60010"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "allapps,tcp,,58088,,8088"
VBoxManage modifyvm "Hortonworks Data Platform Sandbox 2.6" --natpf1 "hue,tcp,,58888,,8888"