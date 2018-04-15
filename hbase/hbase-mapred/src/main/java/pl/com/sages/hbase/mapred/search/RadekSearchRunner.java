package pl.com.sages.hbase.mapred.search;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import pl.com.sages.hbase.api.RadekBooksLoader;
import pl.com.sages.hbase.api.util.HBaseUtil;

/**
 * scp -r hbase-mapred-1.0-SNAPSHOT-jar-with-dependencies.jar login@hdp3:/home/login
 *
 * yarn jar hbase-mapred-1.0-SNAPSHOT-jar-with-dependencies.jar pl.com.sages.hbase.mapred.search.RadekSearchRunner
 */
public class RadekSearchRunner {

    public static final TableName TABLE_NAME = HBaseUtil.getUserTableName("search_index");
    public static final String FAMILY_NAME = RadekBooksLoader.FAMILY_NAME;

    public static void main(String[] args) throws Exception {
        run();
    }

    public static boolean run() throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        Job job = Job.getInstance(configuration, "Search index");
        job.setJarByClass(RadekSearchMapper.class);

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        // Na wejściu tabela filmów
        TableMapReduceUtil.initTableMapperJob(
                RadekBooksLoader.TABLE_NAME,
                scan,
                RadekSearchMapper.class,
                null,
                null,
                job);
        TableMapReduceUtil.initTableReducerJob(
                TABLE_NAME.getNameAsString(),
                null,
                job);
        // Brak reduktora -> patrz implementację Mapper'a
        job.setNumReduceTasks(0);

        return job.waitForCompletion(true);
    }

}
