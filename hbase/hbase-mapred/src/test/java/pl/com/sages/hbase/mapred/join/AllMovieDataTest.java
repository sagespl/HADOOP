package pl.com.sages.hbase.mapred.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;
import pl.com.sages.hbase.api.loader.LoadMovieData;
import pl.com.sages.hbase.api.loader.LoadMovieRatingData;
import pl.com.sages.hbase.mapred.movies.AverageRatingMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class AllMovieDataTest {

    public static final String TABLE_NAME = AllMovieDataMapper.TABLE_NAME;
    public static final String FAMILY_NAME = AllMovieDataMapper.FAMILY_NAME;

    private Configuration configuration = HBaseConfiguration.create();

    @Before
    public void before() throws IOException {
        HBaseAdmin admin = new HBaseAdmin(configuration);

        boolean exists = admin.tableExists(TABLE_NAME);
        if (exists) {
            admin.disableTable(TABLE_NAME);
            admin.deleteTable(TABLE_NAME);
        }

        // tworzenie tabeli HBase
        HTableDescriptor table = new HTableDescriptor(TABLE_NAME);
        table.addFamily(new HColumnDescriptor(FAMILY_NAME));
        table.addFamily(new HColumnDescriptor(LoadMovieData.FAMILY_NAME));
        table.addFamily(new HColumnDescriptor(LoadMovieRatingData.FAMILY_NAME));

        admin.createTable(table);
    }

    @Test
    public void shouldJoinTables() throws Exception {
        //given

        Job job = new Job(configuration, "All movie data");
        job.setJarByClass(AverageRatingMapper.class);

        List<Scan> scans = new ArrayList<>();

        Scan scan1 = new Scan();
        scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(LoadMovieData.TABLE_NAME));
        scans.add(scan1);

        Scan scan2 = new Scan();
        scan2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("ratingaverage"));
        scans.add(scan2);

        TableMapReduceUtil.initTableMapperJob(scans,
                AllMovieDataMapper.class,
                null,
                null,
                job);
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
