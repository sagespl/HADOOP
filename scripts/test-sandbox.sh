#!/usr/bin/env bash

hdfs dfs -ls /user

cd /home/sages/repository/HADOOP
mvn clean install -Puber,sandbox