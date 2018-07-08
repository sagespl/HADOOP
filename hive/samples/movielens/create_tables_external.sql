
drop table if exists emovies;

CREATE EXTERNAL TABLE IF NOT EXISTS emovies (
MovieID STRING,
Title STRING,
Genres STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '@'
LOCATION '/dane/movielens/hive/movies';


drop table if exists eratings;

CREATE EXTERNAL TABLE IF NOT EXISTS eratings (
UserID STRING,
MovieID STRING,
Rating STRING,
Time STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '@'
LOCATION '/dane/movielens/hive/ratings';


drop table if exists etags;

CREATE EXTERNAL TABLE IF NOT EXISTS etags (
UserID STRING,
MovieID STRING,
Tag STRING,
Time STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '@'
LOCATION '/dane/movielens/hive/tags';

show tables;