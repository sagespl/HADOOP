package pl.com.sages.hbase.mapred.movies;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;
import pl.com.sages.hbase.api.dao.RatingDao;
import pl.com.sages.hbase.mapred.file.RatingExportReducer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Wyliczenie średniej oceny dla filmu i zapisanie jej w pliku
 */
public class AverageRatingToFileExternalTest {

    @Test
    public void shouldRunMapReduce() throws Exception {
        //given
        Configuration configuration = HBaseConfiguration.create();
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
        // reduktor standardowo jak w zwykłym MR
        job.setReducerClass(RatingExportReducer.class);
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job, new Path("/tmp/mr/mySummaryFile_" + System.currentTimeMillis()));

        //when
        boolean succeeded = job.waitForCompletion(true);

        //then
        assertThat(succeeded).isTrue();
    }

}