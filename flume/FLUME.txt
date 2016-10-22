
cd /etc/flume/conf/
flume-ng agent --conf-file HADOOP_PROJECT/flume/src/main/resources/fromAvroToHdfs.conf -n agent -Dflume.root.logger=DEBUG,INFO,console