package pl.com.sages.hbase.mapred.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;
import pl.com.sages.hbase.api.loader.LoadMovieData;
import pl.com.sages.hbase.api.loader.TableFactory;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class FilterMapperTest {

    public static final String TABLE_NAME = "somemovies";
    public static final String FAMILY_NAME = "movies";

    private Configuration configuration = HBaseConfiguration.create();

    @Before
    public void before() throws IOException {
        TableFactory.recreateTable(configuration, TABLE_NAME, FAMILY_NAME);
    }

    @Test
    public void shouldRunMapReduce() throws Exception {
        //given
        Job job = new Job(configuration, "Movie Copy");
        job.setJarByClass(FilterMapper.class);

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        scan.addFamily(Bytes.toBytes(LoadMovieData.FAMILY_NAME));

        TableMapReduceUtil.initTableMapperJob(
                LoadMovieData.TABLE_NAME,
                scan,
                FilterMapper.class,
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

        HTableInterface filteredTable = new HTable(configuration, TABLE_NAME);

        scan = new Scan();
        scan.addFamily(Bytes.toBytes(FAMILY_NAME));

        int count = 0;
        ResultScanner results = filteredTable.getScanner(scan);
        for (Result result : results) {
            byte[] id = result.getRow();
            byte[] title = result.getValue(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(LoadMovieData.TITLE));
//            System.out.println(Bytes.toString(id) + " " + Bytes.toString(title));
            count++;
        }

        filteredTable.close();

        assertThat(count).isGreaterThan(1000);
    }

}