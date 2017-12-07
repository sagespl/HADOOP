use sages;

INSERT OVERWRITE DIRECTORY '${OUTPUT}'
select gen, count(1) as num from movies
LATERAL VIEW explode(split(genres,"\\|")) latview AS gen
GROUP BY gen;