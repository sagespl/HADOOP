package pl.com.sages.hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static pl.com.sages.hbase.api.conf.HbaseConfigurationFactory.getConfiguration;

public class HbaseNewApiTest {

    public static final String TABLE_NAME_STRING = "test_" + System.currentTimeMillis();
    public static final TableName TABLE_NAME = TableName.valueOf(TABLE_NAME_STRING);
    public static final String FAMILY_NAME = "info";

    private Admin admin;
    private Configuration configuration;
    private Connection connection;

    @Before
    public void createTable() throws Exception {
        configuration = getConfiguration();
        //admin = new HBaseAdmin(configuration);

        connection = ConnectionFactory.createConnection(getConfiguration());
        admin = connection.getAdmin();


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
        if (admin != null ) admin.close();
        if (connection != null) connection.close();
    }

    @Test
    public void shouldPutDataOnHbase() throws Exception {
        //given
        Table users = connection.getTable(TABLE_NAME);
        Put put = new Put(Bytes.toBytes("id"));
        put.addColumn(Bytes.toBytes(FAMILY_NAME),
                      Bytes.toBytes("cell"),
                      Bytes.toBytes("nasza testowa wartość"));

        //when
        users.put(put);

        //then
    }

}
