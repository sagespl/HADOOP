
set hive.cli.print.current.db=true;

-- wyczyszczenie bazy danych
use radek;
drop table if exists movies;
drop table if exists ratings;
drop table if exists tags;
drop table if exists emovies;
drop table if exists eratings;
drop table if exists etags;

-- stworzenie bazy danych
DROP database if exists radek;
CREATE database radek;
use radek;




-- tabela filmów
drop table if exists movies;

CREATE TABLE IF NOT EXISTS movies (
MovieID STRING,
Title STRING,
Genres STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '@';

LOAD DATA LOCAL INPATH '/home/radek/dane/movielens/hive/movies/movies.dat'
OVERWRITE INTO TABLE movies;

-- tabela ocen
drop table if exists ratings;

CREATE TABLE IF NOT EXISTS ratings (
UserID STRING,
MovieID STRING,
Rating STRING,
Time STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '@';

LOAD DATA LOCAL INPATH '/home/radek/dane/movielens/hive/ratings/ratings.dat'
OVERWRITE INTO TABLE ratings;

-- tabela takgów
drop table if exists tags;

CREATE TABLE IF NOT EXISTS tags (
UserID STRING,
MovieID STRING,
Tag STRING,
Time STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '@';

LOAD DATA LOCAL INPATH '/home/radek/dane/movielens/hive/tags/tags.dat'
OVERWRITE INTO TABLE tags;





drop table if exists emovies;

CREATE EXTERNAL TABLE IF NOT EXISTS emovies (
MovieID STRING,
Title STRING,
Genres STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '@'
LOCATION '/user/radek/dane/movielens/hive/movies';


drop table if exists eratings;

CREATE EXTERNAL TABLE IF NOT EXISTS eratings (
UserID STRING,
MovieID STRING,
Rating STRING,
Time STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '@'
LOCATION '/user/radek/dane/movielens/hive/ratings';


drop table if exists etags;

CREATE EXTERNAL TABLE IF NOT EXISTS etags (
UserID STRING,
MovieID STRING,
Tag STRING,
Time STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '@'
LOCATION '/user/radek/dane/movielens/hive/tags';

show tables;