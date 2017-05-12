package pl.com.sages.hbase.mapred.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;
import pl.com.sages.hbase.api.dao.MovieDao;
import pl.com.sages.hbase.api.dao.RatingDao;
import pl.com.sages.hbase.api.util.HBaseTableBuilder;
import pl.com.sages.hbase.mapred.filter.FilterMapper;
import pl.com.sages.hbase.mapred.movies.AverageRatingMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@org.junit.Ignore
public class UnionExternalTest {

    public static final String TABLE_NAME = "movies_with_rating";
    public static final String FAMILY_NAME = "movies_with_rating";

    private Configuration configuration = HBaseConfiguration.create();

    @Before
    public void before() throws IOException {
        new HBaseTableBuilder()
                .withTable(TABLE_NAME)
                .withFamily(FAMILY_NAME)
                .withFamily(MovieDao.CF)
                .withFamily(RatingDao.CF)
                .build();
    }

    @Test
    public void shouldJoinTables() throws Exception {
        //given

        Job job = Job.getInstance(configuration, "Joins");
        job.setJarByClass(AverageRatingMapper.class);

        List<Scan> scans = new ArrayList<>();

        Scan scan1 = new Scan();
        scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, MovieDao.TABLE.toBytes());
        scans.add(scan1);

        Scan scan2 = new Scan();
        scan2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, RatingDao.TABLE.toBytes());
        scans.add(scan2);

        TableMapReduceUtil.initTableMapperJob(scans,
                FilterMapper.class,
                null,
                null,
                job);
//        FileOutputFormat.setOutputPath(job, new Path("/tmp/sages/movies_with_ratings_" + System.currentTimeMillis()));
        TableMapReduceUtil.initTableReducerJob(
                TABLE_NAME,
                null,
                job);
        job.setNumReduceTasks(0);

        //when
        boolean succeeded = job.waitForCompletion(true);

        //then
        assertThat(succeeded).isTrue();
    }

}
