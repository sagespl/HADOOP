package pl.com.sages.hbase.mapred.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;
import pl.com.sages.hbase.api.dao.MovieDao;
import pl.com.sages.hbase.api.util.HBaseUtil;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class FilterMapperExternalTest {

    public static final TableName TABLE_NAME = HBaseUtil.getUserTableName("movies_comedy");
    public static final String FAMILY_NAME = Bytes.toString(MovieDao.CF);

    private Configuration configuration = HBaseConfiguration.create();

    @Before
    public void before() throws IOException {
        HBaseUtil.recreateTable(TABLE_NAME, FAMILY_NAME);
    }

    @Test
    public void shouldRunMapReduce() throws Exception {
        //given
        Job job = Job.getInstance(configuration, "Movie Copy");
        job.setJarByClass(FilterMapper.class);

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        scan.addFamily(MovieDao.CF);

        TableMapReduceUtil.initTableMapperJob(
                MovieDao.TABLE,
                scan,
                FilterMapper.class,
                null,
                null,
                job);
        TableMapReduceUtil.initTableReducerJob(
                TABLE_NAME.getNameAsString(),
                null,
                job);
        job.setNumReduceTasks(0);

        //when
        boolean succeeded = job.waitForCompletion(true);

        //then
        assertThat(succeeded).isTrue();

        Connection connection = ConnectionFactory.createConnection(configuration);
        Table filteredTable = connection.getTable(TABLE_NAME);
        scan = new Scan();
        scan.addFamily(Bytes.toBytes(FAMILY_NAME));

        int count = 0;
        ResultScanner results = filteredTable.getScanner(scan);
        for (Result result : results) {
            byte[] id = result.getRow();
            byte[] title = result.getValue(Bytes.toBytes(FAMILY_NAME), MovieDao.TITLE);
            byte[] genres = result.getValue(Bytes.toBytes(FAMILY_NAME), MovieDao.GENRES);
            System.out.println(Bytes.toString(id) + " " + Bytes.toString(title) + " " + Bytes.toString(genres));
            count++;
        }

        filteredTable.close();

        assertThat(count).isGreaterThan(1000);
    }

}