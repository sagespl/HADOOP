
drop table if exists movies;

CREATE TABLE IF NOT EXISTS movies (
MovieID STRING,
Title STRING,
Genres STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '@';

LOAD DATA INPATH '/user/sages/dane/movie/movies.dat'
OVERWRITE INTO TABLE movies;

select * from movies limit 10;