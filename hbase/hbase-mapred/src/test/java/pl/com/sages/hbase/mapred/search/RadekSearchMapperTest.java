package pl.com.sages.hbase.mapred.search;

import org.junit.Before;
import org.junit.Test;
import pl.com.sages.hbase.api.util.ConnectionHandler;
import pl.com.sages.hbase.api.util.HBaseTableUtil;
import pl.com.sages.hbase.api.util.HBaseUtil;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class RadekSearchMapperTest {

    @Before
    public void before() throws IOException {
        HBaseUtil.recreateTable(RadekSearchRunner.TABLE_NAME, RadekSearchRunner.FAMILY_NAME);
    }

    @Test
    public void shouldRunMapReduce() throws Exception {
        //given

        //when
        boolean succeeded = RadekSearchRunner.run();

        //then
        assertThat(succeeded).isTrue();
        assertThat(ConnectionHandler.getConnection().getAdmin().tableExists(RadekSearchRunner.TABLE_NAME)).isTrue();
        assertThat(HBaseTableUtil.countNumberOfRows(RadekSearchRunner.TABLE_NAME)).isGreaterThan(1000);
    }

}