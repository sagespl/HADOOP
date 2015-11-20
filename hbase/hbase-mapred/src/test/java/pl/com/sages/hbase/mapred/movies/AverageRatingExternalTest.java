package pl.com.sages.hbase.mapred.movies;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;
import pl.com.sages.hbase.api.dao.RatingDao;
import pl.com.sages.hbase.api.util.HBaseUtil;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class AverageRatingExternalTest {

    public static final String TABLE_NAME = "ratings_average";
    public static final String FAMILY_NAME = "ratings_average";

    private Configuration configuration = HBaseConfiguration.create();

    @Before
    public void before() throws IOException {
        HBaseUtil.recreateTable(TABLE_NAME, FAMILY_NAME);
    }

    @Test
    public void shouldRunMapReduce() throws Exception {
        //given
        Job job = Job.getInstance(configuration, "Average Rating");
        job.setJarByClass(AverageRatingMapper.class);

        job.setMapperClass(AverageRatingMapper.class);

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
                TABLE_NAME,
                AverageRatingReducer.class,
                job);

        //when
        boolean succeeded = job.waitForCompletion(true);

        //then
        assertThat(succeeded).isTrue();

        HTableInterface ratingaverage = new HTable(configuration, TABLE_NAME);

        scan = new Scan();
        scan.addFamily(Bytes.toBytes(FAMILY_NAME));

        int count = 0;
        ResultScanner results = ratingaverage.getScanner(scan);
        for (Result result : results) {
            byte[] id = result.getRow();
            byte[] average = result.getValue(Bytes.toBytes(FAMILY_NAME), AverageRatingReducer.AVERAGE);
            System.out.println(Bytes.toInt(id) + " " + Bytes.toDouble(average));
            count++;
        }

        ratingaverage.close();

        assertThat(count).isGreaterThan(1000);
    }

}