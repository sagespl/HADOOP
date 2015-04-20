package pl.com.sages.hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Test;

import static pl.com.sages.hbase.api.conf.HbaseConfigurationFactory.getConfiguration;

public class HbaseConfigurationFactoryTest {

    @Test
    public void shouldCreateConfiguration() throws Exception {
        //given
        Configuration configuration = getConfiguration();

        //when
        HBaseAdmin.checkHBaseAvailable(configuration);

        //then
    }

}