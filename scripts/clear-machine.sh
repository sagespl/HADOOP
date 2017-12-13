#!/bin/sh

echo "Run as root!!!"

yum clean all

rm -rf /hadoop/yarn/local/filecache
rm -rf /hadoop/yarn/local/usercache

rm -rf /home/sages/.m2/repository/pl/com/sages

# hdfs
# hdfs dfs -rm -r -skipTrash /user/sages/*

# hive

# maven repository
# maven project
# /var/log