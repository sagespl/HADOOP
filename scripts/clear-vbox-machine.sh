#!/bin/sh

echo "Run as root!!!"

df -h

# yum
yum clean all

# Maven
rm -rf /home/sages/.m2/repository/pl/com/sages
cd /home/sages/repository/HADOOP
mvn clean

# hdfs
su hdfs -c 'hdfs dfs -rm -r -skipTrash /user/radek'
su hdfs -c 'hdfs dfs -rm -r -skipTrash /user/sages'

# yarn
rm -rf /hadoop/yarn/local/filecache
rm -rf /hadoop/yarn/local/usercache

# logi
# /var/log

df -h