package pl.com.sages.hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static pl.com.sages.hbase.api.util.HbaseConfigurationFactory.getConfiguration;

public class HbaseApiExternalTest {

    private static final TableName TEST_TABLE_NAME = TableName.valueOf("test_users_" + System.currentTimeMillis());
    private static final String TEST_FAMILY_NAME = "info";

    private Admin admin;
    private Connection connection;

    @Before
    public void createTestTable() throws Exception {
        Configuration configuration = getConfiguration();
        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();

        HTableDescriptor table = new HTableDescriptor(TEST_TABLE_NAME);

        HColumnDescriptor columnFamily = new HColumnDescriptor(TEST_FAMILY_NAME);
        columnFamily.setMaxVersions(10);
        table.addFamily(columnFamily);

        admin.createTable(table);
    }

    @After
    public void deleteTable() throws Exception {
        if (admin != null) {
            admin.disableTable(TEST_TABLE_NAME);
            admin.deleteTable(TEST_TABLE_NAME);
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void shouldPutDataOnHbase() throws Exception {
        //given
        Table users = connection.getTable(TEST_TABLE_NAME);
        Put put = new Put(Bytes.toBytes("id"));
        put.addColumn(Bytes.toBytes(TEST_FAMILY_NAME),
                Bytes.toBytes("cell"),
                Bytes.toBytes("nasza testowa wartość"));

        //when
        users.put(put);

        //then
    }

    @Test
    public void shouldGetDataFromHbase() throws Exception {
        //given
        Table users = connection.getTable(TEST_TABLE_NAME);

        String id = "id";
        String column = "cell";
        String value1 = "nasza testowa wartość";
        String value2 = "nasza testowa wartość 2";

        Put put = new Put(Bytes.toBytes(id));
        put.addColumn(Bytes.toBytes(TEST_FAMILY_NAME),
                Bytes.toBytes(column),
                Bytes.toBytes(value1));
        users.put(put);

        put = new Put(Bytes.toBytes(id));
        put.addColumn(Bytes.toBytes(TEST_FAMILY_NAME),
                Bytes.toBytes(column),
                Bytes.toBytes(value2));
        users.put(put);

        //when
        Get get = new Get(Bytes.toBytes(id));
        get.setMaxVersions(10);
        Result result = users.get(get);

        //then
        assertThat(value2).isEqualToIgnoringCase(Bytes.toString(result.getValue(Bytes.toBytes(TEST_FAMILY_NAME), Bytes.toBytes(column))));

        List<Cell> columnCells = result.getColumnCells(Bytes.toBytes(TEST_FAMILY_NAME), Bytes.toBytes(column));
        assertThat(value2).isEqualToIgnoringCase(Bytes.toString(CellUtil.cloneValue(columnCells.get(0))));
        assertThat(value1).isEqualToIgnoringCase(Bytes.toString(CellUtil.cloneValue(columnCells.get(1))));
    }

    @Test
    public void shouldDeleteDataFromHbase() throws Exception {
        //given
        Table users = connection.getTable(TEST_TABLE_NAME);

        String id = "id";
        String column = "cell";
        String value1 = "nasza testowa wartość";
        String value2 = "nasza testowa wartość 2";

        Put put = new Put(Bytes.toBytes(id));
        put.addColumn(Bytes.toBytes(TEST_FAMILY_NAME),
                Bytes.toBytes(column),
                Bytes.toBytes(value1));
        users.put(put);

        put = new Put(Bytes.toBytes(id));
        put.addColumn(Bytes.toBytes(TEST_FAMILY_NAME),
                Bytes.toBytes(column),
                Bytes.toBytes(value2));
        users.put(put);

        //when
        Delete delete = new Delete(Bytes.toBytes(id));
        delete.addColumn(Bytes.toBytes(TEST_FAMILY_NAME), Bytes.toBytes(column));
        users.delete(delete);

        //then
        Get get = new Get(Bytes.toBytes(id));
        get.setMaxVersions(10);
        Result result = users.get(get);

        assertThat(value1).isEqualToIgnoringCase(Bytes.toString(result.getValue(Bytes.toBytes(TEST_FAMILY_NAME), Bytes.toBytes(column))));
    }

}
