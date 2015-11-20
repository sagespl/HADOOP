use sages;

DROP TABLE IF EXISTS moviegenresstats;

create table moviegenresstats
row format delimited fields terminated by '|'
as
select gen, count(1) as num from movies
LATERAL VIEW explode(split(genres,"\\|")) latview AS gen
GROUP BY gen;

INSERT OVERWRITE DIRECTORY '${OUTPUT}'
select * from moviegenresstats;