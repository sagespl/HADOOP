A = LOAD 'sales_bronx_raw' USING org.apache.hive.hcatalog.pig.HCatLoader();
B = LIMIT A 10;
DUMP B;