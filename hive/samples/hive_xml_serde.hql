CREATE TABLE product_xml(
id STRING,
name STRING,
category STRING,
price DOUBLE,
stock BIGINT,
features map<string,string>)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.id"="/product/@id",
"column.xpath.name"="/product/name/text()",
"column.xpath.category"="/product/category/text()",
"column.xpath.price"="/product/price/text()",
"column.xpath.stock"="/product/stock/text()",
"column.xpath.features"="/product/features/*"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
TBLPROPERTIES (
"xmlinput.start"="<product id",
"xmlinput.end"="</product>"
);
