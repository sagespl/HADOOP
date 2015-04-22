DROP TABLE IF EXISTS sales_bronx_raw;

DROP TABLE IF EXISTS sales_bronx;

-- CREATE EXTERNAL TABLE sales_bronx_raw (lines string)
-- ROW FORMAT DELIMITED LINES TERMINATED BY '\n'
-- STORED AS TEXTFILE
-- LOCATION '/user/sages/rollingsales/';

CREATE EXTERNAL TABLE sales_bronx_raw (lines string)
LOCATION '/user/sages/rollingsales/';

SELECT * FROM sales_bronx_raw LIMIT 10;

CREATE TABLE sales_bronx AS SELECT
regexp_extract(lines, '^(?:([^,]*)\,?){2}', 1) hood,
regexp_extract(lines, '^(?:([^,]*)\,?){3}', 1) type,
regexp_extract(lines, '^(?:([^,]*)\,?){20}', 1) price
FROM sales_bronx_raw;

SELECT * FROM sales_bronx LIMIT 10;

SELECT
type as type_of_house,
hood,
COUNT(*) as house_count,
AVG(cast(regexp_replace(price, '[^0-9]', '') AS BIGINT))  as avg_price
FROM sales_bronx GROUP BY type, hood;


