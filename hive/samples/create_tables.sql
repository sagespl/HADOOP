
set hive.cli.print.current.db=true;

-- stworzenie bazy danych
DROP database if exists sages;
CREATE database sages;
use sages;




-- tabela filmów
drop table if exists movies;

CREATE TABLE IF NOT EXISTS movies (
MovieID STRING,
Title STRING,
Genres STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '@';

LOAD DATA LOCAL INPATH '/home/sages/dane/movie/movies.dat'
OVERWRITE INTO TABLE movies;

-- tabela ocen
drop table if exists ratings;

CREATE TABLE IF NOT EXISTS ratings (
UserID STRING,
MovieID STRING,
Rating STRING,
Time STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '@';

LOAD DATA LOCAL INPATH '/home/sages/dane/movie/ratings.dat'
OVERWRITE INTO TABLE ratings;

-- tabela takgów
drop table if exists tags;

CREATE TABLE IF NOT EXISTS tags (
UserID STRING,
MovieID STRING,
Tag STRING,
Time STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '@';

LOAD DATA LOCAL INPATH '/home/sages/dane/movie/tags.dat'
OVERWRITE INTO TABLE tags;





drop table if exists emovies;

CREATE EXTERNAL TABLE IF NOT EXISTS emovies (
MovieID STRING,
Title STRING,
Genres STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '@'
LOCATION '/user/sages/dane/movie/movies';


drop table if exists eratings;

CREATE EXTERNAL TABLE IF NOT EXISTS eratings (
UserID STRING,
MovieID STRING,
Rating STRING,
Timestamp STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '@'
LOCATION '/user/sages/dane/movie/ratings';


drop table if exists etags;

CREATE EXTERNAL TABLE IF NOT EXISTS etags (
UserID STRING,
MovieID STRING,
Tag STRING,
Timestamp STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '@'
LOCATION '/user/sages/dane/movie/tags';



show tables;

select * from movies limit 10;
select * from emovies limit 10;
select * from ratings limit 10;
select * from eratings limit 10;
select * from tags limit 10;
select * from etags limit 10;