package pl.com.sages.hbase.mapred.movies;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import pl.com.sages.hbase.api.dao.RatingDao;
import pl.com.sages.hbase.api.util.HBaseUtil;

import static pl.com.sages.hbase.mapred.movies.MovieAverageRatingsConstants.*;

public class AverageRatingToTableRunner {

    public static void main(String[] args) throws Exception {
        HBaseUtil.recreateTable(TABLE_NAME, FAMILY_NAME);

        Configuration configuration = HBaseConfiguration.create();
        configuration.set(AverageRatingReducer.FAMILY, FAMILY_NAME);
        configuration.set(AverageRatingReducer.QUALIFIER, QUALIFIER_NAME);

        Job job = Job.getInstance(configuration, "Average Rating");
        job.setJarByClass(AverageRatingMapper.class);

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        scan.addFamily(RatingDao.CF);

        TableMapReduceUtil.initTableMapperJob(
                RatingDao.TABLE,
                scan,
                AverageRatingMapper.class,
                IntWritable.class,
                DoubleWritable.class,
                job);
        TableMapReduceUtil.initTableReducerJob(
                TABLE_NAME.getNameAsString(),
                AverageRatingReducer.class,
                job);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
