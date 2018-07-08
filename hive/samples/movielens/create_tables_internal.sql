
-- tabela filmów
drop table if exists movies;

CREATE TABLE IF NOT EXISTS movies (
MovieID INT,
Title STRING,
Genres STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '@';

LOAD DATA LOCAL INPATH '/home/sages/repository/dane/movielens/hive/movies/movies.dat'
OVERWRITE INTO TABLE movies;

-- LOAD DATA INPATH '/user/sages/dane/movielens/hive/movies/movies.dat'
-- OVERWRITE INTO TABLE movies;

-- tabela ocen
drop table if exists ratings;

CREATE TABLE IF NOT EXISTS ratings (
UserID STRING,
MovieID STRING,
Rating DOUBLE,
Time STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '@';

LOAD DATA LOCAL INPATH '/home/sages/repository/dane/movielens/hive/ratings/ratings.dat'
OVERWRITE INTO TABLE ratings;

-- tabela takgów
drop table if exists tags;

CREATE TABLE IF NOT EXISTS tags (
UserID STRING,
MovieID STRING,
Tag STRING,
Time STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '@';

LOAD DATA LOCAL INPATH '/home/sages/repository/dane/movielens/hive/tags/tags.dat'
OVERWRITE INTO TABLE tags;

show tables;