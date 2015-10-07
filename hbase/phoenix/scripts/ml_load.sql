DROP TABLE IF EXISTS data;
DROP TABLE IF EXISTS item;
DROP TABLE IF EXISTS users;
CREATE TABLE data (
     user_id INTEGER NOT NULL PRIMARY KEY,
     item_id INTEGER,
     rating INTEGER,
     tstamp BIGINT
     ) SALT_BUCKETS=2;
CREATE TABLE item (
    movie_id INTEGER NOT NULL PRIMARY KEY,
    movie_title VARCHAR,
    release_date VARCHAR,
    video_release_date VARCHAR,
    imdb_url VARCHAR,
    unknown INTEGER,
    action INTEGER,
    adventure INTEGER,
    animation INTEGER,
    childrens INTEGER,
    comedy INTEGER,
    crime INTEGER,
    documentary INTEGER,
    drama INTEGER,
    fantasy INTEGER,
    filmnoir INTEGER,
    horror INTEGER,
    musical INTEGER,
    mystery INTEGER,
    romance INTEGER,
    scifi INTEGER,
    thriller INTEGER,
    war INTEGER,
    western INTEGER
    ) SALT_BUCKETS=2;
CREATE TABLE users (
    user_id INTEGER NOT NULL PRIMARY KEY,
    age INTEGER,
    gender VARCHAR,
    occupation VARCHAR,
    zipcode VARCHAR
    ) SALT_BUCKETS=2;
