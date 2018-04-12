package pl.com.sages.hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import pl.com.sages.hbase.api.util.HBaseUtil;

import java.util.List;

public class HBaseMyApp {

    private static final String NAMESPACE = "sages";
    private static final TableName TABLE_NAME = TableName.valueOf(NAMESPACE,"java");
    private static final String FAMILY_NAME = "cf";

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();

        // 0. usunac tabele java jesli istnieje
        if (admin.tableExists(TABLE_NAME)) {
            admin.disableTable(TABLE_NAME);
            admin.deleteTable(TABLE_NAME);
        }

        HBaseUtil.createNamespaceIfNotExists(NAMESPACE, admin);
        // 1. stworzyc tabele java (wersji wiecej niz 1)
        admin.createTable(new HTableDescriptor(TABLE_NAME)
                .addFamily(new HColumnDescriptor(FAMILY_NAME).setMaxVersions(10)));

        // 2. dodac to tabeli java nowy wiersz, dwie kolumny, dwie wersje
        Table table = connection.getTable(TABLE_NAME);
        table.put(new Put(Bytes.toBytes("id"))
                .addColumn(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes("cell1"),Bytes.toBytes("nasza testowa wartość 1a"))
                .addColumn(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes("cell2"),Bytes.toBytes("nasza testowa wartość 2a")));
        table.put(new Put(Bytes.toBytes("id"))
                .addColumn(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes("cell1"),Bytes.toBytes("nasza testowa wartość 1b"))
                .addColumn(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes("cell2"),Bytes.toBytes("nasza testowa wartość 2b")));

        // 3. uzyc get'a do pobrania i wyswietlenia wiersza
        Result result = table.get(new Get(Bytes.toBytes("id")).setMaxVersions(10));
        List<Cell> columnCells = result.listCells();
        for (Cell columnCell : columnCells) {
            System.out.println(Bytes.toString(CellUtil.cloneValue(columnCell)));
        }
        // 3*. uzyc scana'a do pobrania i wyswietlenia wiersza
    }

}
