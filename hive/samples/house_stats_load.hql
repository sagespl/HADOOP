DROP TABLE IF EXISTS sales_bronx_raw;

DROP TABLE IF EXISTS sales_bronx;

CREATE TABLE sales_bronx_raw (lines string);

LOAD DATA INPATH '/user/sages/rollingsales_bronx.csv' OVERWRITE INTO TABLE sales_bronx_raw;

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