A = LOAD 'sales_bronx_raw' USING org.apache.hcatalog.pig.HCatLoader();
B = LIMIT A 10;
DUMP B;