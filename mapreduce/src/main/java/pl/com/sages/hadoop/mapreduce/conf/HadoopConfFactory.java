package pl.com.sages.hadoop.mapreduce.conf;

import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

/**
 * Hadoop Conf Factory
 */
public class HadoopConfFactory {
    public static final String HADOOP_USER = "hue";

    public static Configuration getConfiguration() {
        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS", "hdfs://127.0.0.1:8020/");
        //conf.set("fs.defaultFS", "hdfs://192.168.56.101:8020/");
        conf.set("fs.defaultFS", "hdfs://sandbox:8020");
        conf.set("hadoop.job.ugi", HADOOP_USER);
        return conf;
    }
}
