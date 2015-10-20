package pl.com.sages.hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static pl.com.sages.hbase.api.conf.HbaseConfigurationFactory.getConfiguration;

public class HbaseOldApiExternalTest {

    public static final String TEST_TABLE_NAME = "test_" + System.currentTimeMillis();
    public static final String TEST_FAMILY_NAME = "info";

    private HBaseAdmin admin;
    private Configuration configuration;

    @Before
    public void createTestTable() throws Exception {
        configuration = getConfiguration();
        admin = new HBaseAdmin(configuration);

        HTableDescriptor table = new HTableDescriptor(TEST_TABLE_NAME);
        HColumnDescriptor columnFamily = new HColumnDescriptor(TEST_FAMILY_NAME);
        columnFamily.setMaxVersions(1);
        table.addFamily(columnFamily);

        admin.createTable(table);
    }

    @After
    public void deleteTable() throws Exception {
        admin.disableTable(TEST_TABLE_NAME);
        admin.deleteTable(TEST_TABLE_NAME);
    }

    @Test
    public void shouldPutDataOnHbase() throws Exception {
        //given
        HTableInterface users = new HTable(configuration, TEST_TABLE_NAME);
        Put put = new Put(Bytes.toBytes("id"));
        put.add(Bytes.toBytes(TEST_FAMILY_NAME),
                Bytes.toBytes("cell"),
                Bytes.toBytes("nasza testowa wartość"));

        //when
        users.put(put);

        //then
    }

}
