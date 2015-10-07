#!/bin/bash
ZOOKEEPER="hdpmaster1:/hbase-unsecure"
SQLLINE="/usr/hdp/current/phoenix-client/bin/sqlline.py $ZOOKEEPER"
PSQL="/usr/hdp/current/phoenix-client/bin/psql.py" 

# Creating tables
$SQLLINE ml_load.sql

DATA=ml-100k/u.data
DATA_CSV=data.csv
ITEM=ml-100k/u.item
ITEM_CSV=item.csv
USERS=ml-100k/u.user
USERS_CSV=users.csv

# Make links for psql, which accepts only *.csv
ln -s $DATA $DATA_CSV
ln -s $ITEM $ITEM_CSV
ln -s $USERS $USERS_CSV

# Load CSVs
$PSQL -t DATA  -d $'\t' $ZOOKEEPER $DATA_CSV
$PSQL -t ITEM  -d '|'   $ZOOKEEPER $ITEM_CSV
$PSQL -t USERS -d '|'   $ZOOKEEPER $USERS_CSV

# Cleanup links
rm $DATA_CSV $ITEM_CSV $USERS_CSV

