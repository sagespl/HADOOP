data = LOAD '/data/ml-100k/u.data' USING PigStorage('\t') AS (
    user_id:int,
    item_id:int,
    rating:int,
    timestamp:long
);

item = LOAD '/data/ml-100k/u.item' USING PigStorage('\t') AS (
    movie_id:int,
    movie_title:chararray,
    release_date:chararray,
    video_release_date:chararray,
    imdb_url:chararray,
    unknown:int,
    action:int,
    adventure:int,
    animation:int,
    childrens:int,
    comedy:int,
    crime:int,
    documentary:int,
    drama:int,
    fantasy:int,
    filmnoir:int,
    horror:int,
    musical:int,
    mystery:int,
    romance:int,
    scifi:int,
    thriller:int,
    war:int,
    western:int
);

user = LOAD '/data/ml-100k/u.user' USING PigStorage('\t') AS (
    user_id:int,
    age:int,
    gender:int,
    occupation:int,
    zipcode:int
);

user_limit = LIMIT user 10;

DUMP user_limit;

-- Hints:
-- pig -useHCatalog
-- A = LOAD 'tablename' USING org.apache.hcatalog.pig.HCatLoader();
-- STORE my_processed_data INTO 'tablename' USING org.apache.hcatalog.pig.HCatStorer();