package pl.com.sages.hbase.mapred.movies;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;
import pl.com.sages.hbase.api.loader.LoadMovieRatingData;
import pl.com.sages.hbase.api.loader.TableFactory;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class AverageRatingTest {

    public static final String TABLE_NAME = "ratingaverage";
    public static final String FAMILY_NAME = "ratingaverage";

    private Configuration configuration = HBaseConfiguration.create();

    @Before
    public void before() throws IOException {
        TableFactory.recreateTable(configuration, TABLE_NAME, FAMILY_NAME);
    }

    @Test
    public void shouldRunMapReduce() throws Exception {
        //given
        Job job = new Job(configuration, "Average Rating");
        job.setJarByClass(AverageRatingMapper.class);

        job.setMapperClass(AverageRatingMapper.class);

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        scan.addFamily(Bytes.toBytes(LoadMovieRatingData.FAMILY_NAME));

        TableMapReduceUtil.initTableMapperJob(
                LoadMovieRatingData.TABLE_NAME,
                scan,
                AverageRatingMapper.class,
                Text.class,
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
            System.out.println(Bytes.toString(id) + " " + Bytes.toDouble(average));
            count++;
        }

        ratingaverage.close();

        assertThat(count).isGreaterThan(1000);
    }

}