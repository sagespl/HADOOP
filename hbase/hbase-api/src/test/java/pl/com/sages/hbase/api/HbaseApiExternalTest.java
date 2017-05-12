package pl.com.sages.hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import pl.com.sages.hbase.api.util.HBaseUtil;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static pl.com.sages.hbase.api.util.HbaseConfigurationFactory.getConfiguration;

public class HbaseApiExternalTest {

    private static final TableName TEST_TABLE_NAME = HBaseUtil.getUserTableName("hbase_api_test_table");
    private static final String FAMILY_NAME_1 = "cf1";
    private static final String FAMILY_NAME_2 = "cf2";

    private Connection connection;
    private Admin admin;

    @Before
    public void createTestTable() throws Exception {
        Configuration configuration = getConfiguration();
        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();

        if (admin.tableExists(TEST_TABLE_NAME)) {
            admin.disableTable(TEST_TABLE_NAME);
            admin.deleteTable(TEST_TABLE_NAME);
        }

        HTableDescriptor table = new HTableDescriptor(TEST_TABLE_NAME);

        HColumnDescriptor columnFamily1 = new HColumnDescriptor(FAMILY_NAME_1);
        columnFamily1.setMaxVersions(10);
        table.addFamily(columnFamily1);

        HColumnDescriptor columnFamily2 = new HColumnDescriptor(FAMILY_NAME_2);
        columnFamily2.setMaxVersions(10);
        table.addFamily(columnFamily2);

        admin.createTable(table);
    }

    @After
    public void deleteTable() throws Exception {
        if (admin != null) {
//            admin.disableTable(TEST_TABLE_NAME);
//            admin.deleteTable(TEST_TABLE_NAME);
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void shouldPutAndGetDataFromHbase() throws Exception {
        //given
        Table table = connection.getTable(TEST_TABLE_NAME);

        String id = "id";
        String qualifier = "cell";
        String value = "nasza testowa wartość";

        Put put = new Put(Bytes.toBytes(id));
        put.addColumn(Bytes.toBytes(FAMILY_NAME_1),
                Bytes.toBytes(qualifier),
                Bytes.toBytes(value));

        table.put(put);

        //when
        Get get = new Get(Bytes.toBytes(id));
        get.setMaxVersions(10);
        Result result = table.get(get);

        //then
        assertThat(value).isEqualToIgnoringCase(Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_NAME_1), Bytes.toBytes(qualifier))));
    }

    @Test
    public void shouldPutAndGetDataFromHbaseWithVersions() throws Exception {
        //given
        Table table = connection.getTable(TEST_TABLE_NAME);

        String id = "id";
        String qualifier = "cell";
        String value1 = "nasza testowa wartość";
        String value2 = "nasza testowa wartość 2";

        Put put = new Put(Bytes.toBytes(id));
        put.addColumn(Bytes.toBytes(FAMILY_NAME_1),
                Bytes.toBytes(qualifier),
                Bytes.toBytes(value1));
        table.put(put);

        put = new Put(Bytes.toBytes(id));
        put.addColumn(Bytes.toBytes(FAMILY_NAME_1),
                Bytes.toBytes(qualifier),
                Bytes.toBytes(value2));
        table.put(put);

        //when
        Get get = new Get(Bytes.toBytes(id));
        get.setMaxVersions(10);
        Result result = table.get(get);

        //then
        assertThat(value2).isEqualToIgnoringCase(Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_NAME_1), Bytes.toBytes(qualifier))));

        List<Cell> columnCells = result.getColumnCells(Bytes.toBytes(FAMILY_NAME_1), Bytes.toBytes(qualifier));
        assertThat(value2).isEqualToIgnoringCase(Bytes.toString(CellUtil.cloneValue(columnCells.get(0))));
        assertThat(value1).isEqualToIgnoringCase(Bytes.toString(CellUtil.cloneValue(columnCells.get(1))));
    }

    @Test
    public void shouldDeleteDataFromHbase() throws Exception {
        //given
        Table table = connection.getTable(TEST_TABLE_NAME);

        String id = "id";
        String qualifier = "cell";
        String value1 = "nasza testowa wartosc 1";
        String value2 = "nasza testowa wartosc 2";
        String value3 = "nasza testowa wartosc 3";
        long timestamp = 100;

        put(table, id, FAMILY_NAME_1, qualifier, ++timestamp, value1);
        put(table, id, FAMILY_NAME_1, qualifier, ++timestamp, value2);
        put(table, id, FAMILY_NAME_1, qualifier, ++timestamp, value3);

        timestamp = 200;
        put(table, id, FAMILY_NAME_2, qualifier, ++timestamp, value1);
        put(table, id, FAMILY_NAME_2, qualifier, ++timestamp, value2);
        put(table, id, FAMILY_NAME_2, qualifier, ++timestamp, value3);


        //when
        // bez dodatkowych metod usuwa cały wiersz (DeleteFamily dla każdej rodziny z aktualnym timestamp)
        Delete delete = new Delete(Bytes.toBytes(id));
        // delete latest versio of qualifier (Delete)
//        delete.addColumn(Bytes.toBytes(FAMILY_NAME_1), Bytes.toBytes(qualifier));
        // delete selected version (Delete)
//        delete.addColumn(Bytes.toBytes(FAMILY_NAME_1), Bytes.toBytes(qualifier), 102);

        // delete all versions of column (DeleteColumn)
//        delete.addColumns(Bytes.toBytes(FAMILY_NAME_1), Bytes.toBytes(qualifier));
        // delete all versions of column with less or equal timestamp (DeleteColumn)
//        delete.addColumns(Bytes.toBytes(FAMILY_NAME_1), Bytes.toBytes(qualifier), 102);

//        delete.addFamily(Bytes.toBytes(FAMILY_NAME_1));
        delete.addFamily(Bytes.toBytes(FAMILY_NAME_1), 102);
        table.delete(delete);

        //then
//        Get get = new Get(Bytes.toBytes(id));
//        get.setMaxVersions(10);
//        Result result = table.get(get);

//        assertThat(value1).isEqualToIgnoringCase(Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(qualifier))));
    }

    private void put(Table table, String id, String family, String qualifier, long timestamp, String value) throws Exception {
        Put put = new Put(Bytes.toBytes(id));
        put.addColumn(Bytes.toBytes(family),
                Bytes.toBytes(qualifier),
                timestamp,
                Bytes.toBytes(value));
        table.put(put);
    }

}
