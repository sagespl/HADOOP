raw = LOAD '$in_file' USING PigStorage(',');
by_type = GROUP raw BY $2;
-- DUMP by_type;
count_by_type = FOREACH by_type GENERATE COUNT(raw) AS tot, group;
ftd = FILTER count_by_type BY tot >= 100;
DUMP count_by_type;

