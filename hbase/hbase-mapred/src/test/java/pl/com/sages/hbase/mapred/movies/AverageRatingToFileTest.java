package pl.com.sages.hbase.mapred.movies;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Before;
import org.junit.Test;
import pl.com.sages.hbase.api.loader.LoadMovieRatingData;
import pl.com.sages.hbase.api.loader.TableFactory;
import pl.com.sages.hbase.mapred.file.RatingExportReducer;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class AverageRatingToFileTest {

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
        job.setReducerClass(RatingExportReducer.class);
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job, new Path("/tmp/mr/mySummaryFile_" + System.currentTimeMillis()));

        //when
        boolean succeeded = job.waitForCompletion(true);

        //then
        assertThat(succeeded).isTrue();
    }

}