package pl.com.sages.hbase.mapred.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;
import pl.com.sages.hbase.api.dao.MovieDao;
import pl.com.sages.hbase.api.dao.RatingDao;
import pl.com.sages.hbase.api.util.HBaseTableBuilder;
import pl.com.sages.hbase.mapred.movies.AverageRatingMapper;
import pl.com.sages.hbase.mapred.movies.MovieAverageRatingsConstants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class AllMovieDataExternalTest {

    private static final TableName TABLE_NAME = AllMovieDataMapper.TABLE_NAME;
    private static final String FAMILY_NAME = AllMovieDataMapper.FAMILY_NAME;

    private Configuration configuration = HBaseConfiguration.create();

    @Before
    public void before() throws IOException {
        new HBaseTableBuilder()
                .withTable(TABLE_NAME)
                .withFamily(FAMILY_NAME)
                .withFamily(MovieDao.CF)
                .withFamily(RatingDao.CF)
                .rebuild();
    }

    @Test
    public void shouldJoinTables() throws Exception {
        //given
        Job job = Job.getInstance(configuration, "All movie data");
        job.setJarByClass(AverageRatingMapper.class);

        List<Scan> scans = new ArrayList<>();

        Scan scan1 = new Scan();
        scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, MovieDao.TABLE.toBytes());
        scans.add(scan1);

        Scan scan2 = new Scan();
        scan2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, MovieAverageRatingsConstants.TABLE_NAME.toBytes());
        scans.add(scan2);

        TableMapReduceUtil.initTableMapperJob(scans,
                AllMovieDataMapper.class,
                null,
                null,
                job);
        TableMapReduceUtil.initTableReducerJob(
                TABLE_NAME.getNameAsString(),
                null,
                job);
        // Brak redukcji, join w maperze!
        job.setNumReduceTasks(0);

        //when
        boolean succeeded = job.waitForCompletion(true);

        //then
        assertThat(succeeded).isTrue();
    }

}
