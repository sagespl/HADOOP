#!/bin/sh

echo "Run as root!!!"

yum clean all

rm -rf /hadoop/yarn/local/filecache
rm -rf /hadoop/yarn/local/usercache

rm -rf /home/sages/.m2/repository/pl/com/sages

df -h

# hdfs
su hdfs -c 'hdfs dfs -rm -r -skipTrash /user/radek'
su hdfs -c 'hdfs dfs -rm -r -skipTrash /user/sages'
su hdfs -c 'hdfs dfs -mkdir /user/sages'
su hdfs -c 'hdfs dfs -chown sages /user/sages'

# hive

# maven repository
# maven project
# /var/log