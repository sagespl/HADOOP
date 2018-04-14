package pl.com.sages.hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import pl.com.sages.hbase.api.util.HBaseUtil;

import java.text.MessageFormat;
import java.util.List;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static pl.com.sages.hbase.api.util.HbaseConfigurationFactory.getConfiguration;

public class RadekHBaseApp {

    //    private static final TableName TABLE_NAME = TableName.valueOf("radek","java");
    private static final TableName TABLE_NAME = HBaseUtil.getUserTableName("java");
    private static final String FAMILY_NAME = "cf1";

    public static void main(String[] args) throws Exception {
        Configuration configuration = getConfiguration();
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();

        // 0. usunac tabelkę java jeśli istnieje
        if (admin.tableExists(TABLE_NAME)) {
            admin.disableTable(TABLE_NAME);
            admin.deleteTable(TABLE_NAME);
        }

        // 1. stworzyc tabelkę java w swoim namespace
        HBaseUtil.createNamespaceIfNotExists(TABLE_NAME.getNamespaceAsString());
        admin.createTable(new HTableDescriptor(TABLE_NAME)
                .addFamily(new HColumnDescriptor(FAMILY_NAME).setMaxVersions(10)));

        // 2. dodać do tabeli java dwa wiersze: dwie kolumny, każda dwie wartości
        Table table = connection.getTable(TABLE_NAME);

        for (int i = 0; i < 2; i++) {
            table.put(new Put(toBytes(i))
                    .addColumn(toBytes(FAMILY_NAME), toBytes("kol1"), toBytes("wersja 1a"))
                    .addColumn(toBytes(FAMILY_NAME), toBytes("kol2"), toBytes("wersja 1b")));
            table.put(new Put(toBytes(i))
                    .addColumn(toBytes(FAMILY_NAME), toBytes("kol1"), toBytes("wersja 2a"))
                    .addColumn(toBytes(FAMILY_NAME), toBytes("kol2"), toBytes("wersja 2b")));
        }

        // 3. pobrać dane z bazy i wyświetlić na konsoli (system out)
        // a) GET
        // b*) Scan
        ResultScanner scanner = table.getScanner(new Scan().setMaxVersions(10));
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {

                int id = Bytes.toInt(CellUtil.cloneRow(cell));
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                long timestamp = cell.getTimestamp();

                System.out.println(MessageFormat.format("id: {0}, col: {1}:{2}, value: {3}, t: {4}",
                        id, family, qualifier, value, timestamp));
            }
        }

    }

}
