raw = LOAD '/user/root/rollingsales_bronx.csv';
by_type = GROUP raw BY $2;
DUMP by_type;
count_by_type = FOREACH by_type GENERATE COUNT(raw) AS tot, group;
ftd = FILTER count_by_type BY tot >= 100;
DUMP count_by_type;

A = LOAD 'sales_bronx_raw' USING org.apache.hcatalog.pig.HCatLoader();
B = LIMIT A 10;
DUMP B;