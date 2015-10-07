-- from shell: export HIVE_AUX_JARS_PATH=<folder_path>
ADD JAR ../target/hive-1.0-SNAPSHOT.jar;

CREATE TEMPORARY FUNCTION myMD5 as 'pl.com.sages.hadoop.hive.MD5HiveUdf';

SELECT myMD5(type) FROM sales_bronx LIMIT 10;