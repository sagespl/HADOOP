package pl.com.sages.hbase.mapred.users.count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.junit.Test;
import pl.com.sages.hbase.api.loader.LoadMovieData;

import static org.assertj.core.api.Assertions.assertThat;

public class CountMoviesTest {

    @Test
    public void shouldCountMovies() throws Exception {
        Configuration configuration = HBaseConfiguration.create();

        // map reduce
        Job job = new Job(configuration, "Count Movies");
        job.setJarByClass(CountMoviesMapper.class);

        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false); // don't set to true for MR jobs
        scan.addColumn(Bytes.toBytes(LoadMovieData.FAMILY_NAME), Bytes.toBytes(LoadMovieData.GENRES));

        // mapper
        TableMapReduceUtil.initTableMapperJob(
                LoadMovieData.TABLE_NAME,
                scan,
                CountMoviesMapper.class,
                ImmutableBytesWritable.class,
                Result.class,
                job);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setNumReduceTasks(0);

        //when
        boolean succeeded = job.waitForCompletion(true);

        //then
        assertThat(succeeded).isTrue();
        assertThat(job.getCounters().findCounter(CountMoviesMapper.Counters.MOVIE_COUNT).getValue()).isEqualTo(5339);
    }

}
