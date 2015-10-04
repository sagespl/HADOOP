DROP TABLE IF EXISTS data_text;
DROP TABLE IF EXISTS item_text;
DROP TABLE IF EXISTS user_text;

DROP TABLE IF EXISTS data;
DROP TABLE IF EXISTS item;
DROP TABLE IF EXISTS user;

CREATE EXTERNAL TABLE data_text (
    user_id INT,
    item_id INT,
    rating INT,
    timestamp BIGINT
    ) STORED AS TEXTFILE
    LOCATION '/user/sages/ml-100k/u.data';

--LOAD DATA INPATH '/user/sages/ml-100k/u.data' OVERWRITE INTO TABLE data;

CREATE TABLE data AS
    SELECT * FROM data_text
    STORED AS ORC;

DROP TABLE data_text;

CREATE EXTERNAL TABLE item_text (
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
    ) STORED AS TEXTFILE
    LOCATION '/user/sages/ml-100k/u.item';

-- LOAD DATA INPATH '/user/sages/ml-100k/u.item' OVERWRITE INTO TABLE item;

CREATE TABLE item AS
    SELECT * FROM item_text
    STORED AS ORC;

DROP TABLE item_text;

CREATE EXTERNAL TABLE user (
    user_id INT,
    age INT,
    gender STRING,
    occupation STRING,
    zipcode STRING
    ) STORED AS TEXTFILE
    LOCATION '/user/sages/ml-100k/u.user';

-- LOAD DATA INPATH '/user/sages/ml-100k/u.user' OVERWRITE INTO TABLE user;

CREATE TABLE user AS
    SELECT * FROM user_text
    STORED AS ORC;

DROP TABLE user_text;
