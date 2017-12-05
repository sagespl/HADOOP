#!/bin/sh

# MovieLens Dataset: https://grouplens.org/datasets/movielens/

cd $HADOOP_DATA
pwd
rm -rf movielens
mkdir movielens
cd movielens

#wget http://files.grouplens.org/datasets/movielens/ml-latest.zip
#wget http://files.grouplens.org/datasets/movielens/ml-20m.zip
wget http://files.grouplens.org/datasets/movielens/ml-10m.zip

#unzip ml-latest.zip
#unzip ml-20m.zip
unzip ml-10m.zip

# Dane z https://dev.mysql.com/doc/index-other.html

cd $HADOOP_DATA
pwd
rm -rf mysql
mkdir mysql
cd mysql

#wget http://downloads.mysql.com/docs/world.sql.gz
#wget http://downloads.mysql.com/docs/world_x-db.tar.gz
wget http://downloads.mysql.com/docs/sakila-db.tar.gz
#wget http://downloads.mysql.com/docs/menagerie-db.tar.gz

# Lektury

cd $HADOOP_DATA
pwd
rm -rf lektury

wget https://github.com/radoslawszmit/BigDataTrainingDataset/blob/master/lektury.tar.gz?raw=true lektury.tar.gz
tar -zxf lektury.tar.gz

# Podsumowanie

du -s -h $HADOOP_DATA
tree -L 2 $HADOOP_DATA

