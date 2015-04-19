CREATE TABLE sales_1 AS SELECT
regexp_extract(lines, '^(?:([^,]*)\,?){2}', 1) hood,
regexp_extract(lines, '^(?:([^,]*)\,?){3}', 1) type,
regexp_extract(lines, '^(?:([^,]*)\,?){20}', 1) price
FROM sales_bronx_raw;


SELECT myMD5(type) FROM aaa LIMIT 10;


SELECT
type as type_of_house,
hood,
COUNT(*) as house_count,
AVG(cast(regexp_replace(price, '[^0-9]', '') AS BIGINT))  as avg_price
FROM sales_1 GROUP BY type, hood;