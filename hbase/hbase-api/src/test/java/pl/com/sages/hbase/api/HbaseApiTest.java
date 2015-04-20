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

public class HbaseApiTest {

    public static final String TABLE_NAME = "test_" + System.currentTimeMillis();
    public static final String FAMILY_NAME = "info";

    private HBaseAdmin admin;
    private Configuration configuration;

    @Before
    public void createTable() throws Exception {
        configuration = getConfiguration();
        admin = new HBaseAdmin(configuration);

        HTableDescriptor table = new HTableDescriptor(TABLE_NAME);
        HColumnDescriptor columnFamily = new HColumnDescriptor(FAMILY_NAME);
        columnFamily.setMaxVersions(1);
        table.addFamily(columnFamily);

        admin.createTable(table);
    }

    @After
    public void deleteTable() throws Exception {
        admin.disableTable(TABLE_NAME);
        admin.deleteTable(TABLE_NAME);
    }

    @Test
    public void shouldPutDataOnHbase() throws Exception {
        //given
        HTableInterface users = new HTable(configuration, TABLE_NAME);
        Put put = new Put(Bytes.toBytes("id"));
        put.add(Bytes.toBytes(FAMILY_NAME),
                Bytes.toBytes("cell"),
                Bytes.toBytes("nasza testowa wartość"));

        //when
        users.put(put);

        //then
    }

}
