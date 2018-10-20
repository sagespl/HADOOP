package pl.com.sages.hbase.api;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import pl.com.sages.hbase.api.util.HBaseUtil;

import java.util.List;

import static pl.com.sages.hbase.api.util.HbaseConfigurationFactory.getConfiguration;

public class HBaseRadekApp {

    private static final TableName TABLE_NAME = HBaseUtil.getUserTableName("java");
    private static final String FAMILY_NAME = "cf";

    public static void main(String[] args) throws Exception {
        // 0. usunąć tabelę "login:java" jeśli istnieje
        // login to namespace
        // 1. stworzyc tabelę "login:java"
        // jedna rodzina, 3 wersje

        Connection connection = ConnectionFactory.createConnection(getConfiguration());
        Admin admin = connection.getAdmin();

        if (admin.tableExists(TABLE_NAME)) {
            admin.disableTable(TABLE_NAME);
            admin.deleteTable(TABLE_NAME);
        }

        HBaseUtil.createNamespaceIfNotExists(TABLE_NAME.getNamespaceAsString());

        admin.createTable(new HTableDescriptor(TABLE_NAME)
                .addFamily(new HColumnDescriptor(FAMILY_NAME).setMaxVersions(3)));


        // 2. dodać do tabeli jeden nowy wiersz, jedna kolumna
        Table table = connection.getTable(TABLE_NAME);

        String id = "id";
        String qualifier = "cell"; // nazwa kolumny w ramach rodzina, pełna nazwa kolumny 'family:qualifier'
        String value1 = "nasza testowa wartość";
        String value2 = "nasza testowa wartość 2";

        table.put(new Put(Bytes.toBytes(id)).addColumn(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(qualifier), Bytes.toBytes(value1)));

        // 3. dodać nową wersję tego samego wiersza (identyczne id)

        table.put(new Put(Bytes.toBytes(id)).addColumn(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(qualifier), Bytes.toBytes(value2)));

        // 4. pobrać i wyświetlić zawartość tabeli/wiersza (także historię)

        Result result = table.get(new Get(Bytes.toBytes(id)).setMaxVersions(10));

        List<Cell> columnCells = result.getColumnCells(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(qualifier));
        System.out.println(Bytes.toString(CellUtil.cloneValue(columnCells.get(0))));
        System.out.println(Bytes.toString(CellUtil.cloneValue(columnCells.get(1))));

    }

}
