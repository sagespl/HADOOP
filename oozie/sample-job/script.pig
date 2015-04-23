data = LOAD '$INPUT' USING PigStorage('\t') AS (word:chararray, count:int);
filtered = FILTER data BY word MATCHES '^[Aa].*';
STORE filtered INTO '$OUTPUT' USING PigStorage('\t');
