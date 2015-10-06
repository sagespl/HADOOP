DROP TABLE IF EXISTS data_text;
DROP TABLE IF EXISTS item_text;
DROP TABLE IF EXISTS users_text;

DROP TABLE IF EXISTS data;
DROP TABLE IF EXISTS item;
DROP TABLE IF EXISTS users;

CREATE EXTERNAL TABLE data_text (
     user_id INT,
     item_id INT,
     rating INT,
     tstamp BIGINT
     ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
     STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH 'ml-100k/u.data' OVERWRITE INTO TABLE data_text;

CREATE TABLE data STORED AS ORC AS
    SELECT * FROM data_text;

DROP TABLE data_text;

CREATE TABLE item_text (
    movie_id INT,
    movie_title STRING,
    release_date STRING,
    video_release_date STRING,
    imdb_url STRING,
    unknown INT,
    action INT,
    adventure INT,
    animation INT,
    childrens INT,
    comedy INT,
    crime INT,
    documentary INT,
    drama INT,
    fantasy INT,
    filmnoir INT,
    horror INT,
    musical INT,
    mystery INT,
    romance INT,
    scifi INT,
    thriller INT,
    war INT,
    western INT
    ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
     STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH 'ml-100k/u.item' OVERWRITE INTO TABLE item_text;

CREATE TABLE item STORED AS ORC AS
    SELECT * FROM item_text;

DROP TABLE item_text;

CREATE TABLE users_text (
    user_id INT,
    age INT,
    gender STRING,
    occupation STRING,
    zipcode STRING
    )  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH 'ml-100k/u.user' OVERWRITE INTO TABLE users_text;

CREATE TABLE users STORED AS ORC AS
    SELECT * FROM users_text;

DROP TABLE users_text;
