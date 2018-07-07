#!/bin/sh

scp -r -P 2222 $HADOOP_DATA/* sages@localhost://home/sages/repository/dane/
ssh sages@localhost -p 2222