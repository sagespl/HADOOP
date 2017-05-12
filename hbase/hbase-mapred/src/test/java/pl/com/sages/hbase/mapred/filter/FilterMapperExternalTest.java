package pl.com.sages.hbase.mapred.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;
import pl.com.sages.hbase.api.dao.MovieDao;
import pl.com.sages.hbase.api.util.ConnectionHandler;
import pl.com.sages.hbase.api.util.HBaseTableUtil;
import pl.com.sages.hbase.api.util.HBaseUtil;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Przykład filtrowania za pomocą MapReduce -> zostawiamy tylko filmy które są komediami
 */
public class FilterMapperExternalTest {

    private static final TableName TABLE_NAME = HBaseUtil.getUserTableName("movies_comedy");
    private static final String FAMILY_NAME = Bytes.toString(MovieDao.CF);

    @Before
    public void before() throws IOException {
        HBaseUtil.recreateTable(TABLE_NAME, FAMILY_NAME);
    }

    @Test
    public void shouldRunMapReduce() throws Exception {
        //given
        Configuration configuration = HBaseConfiguration.create();
        Job job = Job.getInstance(configuration, "Movie Comedy Filter");
        job.setJarByClass(FilterMapper.class);

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        scan.addFamily(MovieDao.CF);

        // Na wejściu tabela filmów
        TableMapReduceUtil.initTableMapperJob(
                MovieDao.TABLE,
                scan,
                FilterMapper.class,
                null,
                null,
                job);
        // Na wyjściu tabela z komediami
        TableMapReduceUtil.initTableReducerJob(
                TABLE_NAME.getNameAsString(),
                null,
                job);
        // Brak reduktora -> patrz implementację Mapper'a
        job.setNumReduceTasks(0);

        //when
        boolean succeeded = job.waitForCompletion(true);

        //then
        assertThat(succeeded).isTrue();
        assertThat(ConnectionHandler.getConnection().getAdmin().tableExists(TABLE_NAME)).isTrue();
        assertThat(HBaseTableUtil.countNumberOfRows(TABLE_NAME)).isGreaterThan(1000);
    }

}