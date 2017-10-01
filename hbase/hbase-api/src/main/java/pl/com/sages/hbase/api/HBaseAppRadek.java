package pl.com.sages.hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import pl.com.sages.hbase.api.util.HBaseUtil;

import java.util.List;

public class HBaseAppRadek {

    private static final TableName JAVA_TABLE = TableName.valueOf("radek", "java"); // radek:java
    private static final String FAMILY_NAME = "cf1";
    private static final String ID = "id";
    private static final String CELL_1 = "cell1";
    private static final String CELL_2 = "cell2";

    public static void main(String[] args) throws Exception {
        System.out.println("HBase Java Application");

        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();

        // 0. Usunąc tabele "java" jesli istnieje
        if (admin.tableExists(JAVA_TABLE)) {
            admin.disableTable(JAVA_TABLE);
            admin.deleteTable(JAVA_TABLE);
        }

        // 1. Stworzyc tabele "java" w swoim namespace (login)
        // -> VERSIONS => 5
        HBaseUtil.createNamespaceIfNotExists(JAVA_TABLE.getNamespaceAsString(), admin);
        admin.createTable(new HTableDescriptor(JAVA_TABLE).addFamily(
                new HColumnDescriptor(FAMILY_NAME).setMaxVersions(5)
        ));

        // 2. Dodać dwa puty do dwóch różnych kolumn
        Table table = connection.getTable(JAVA_TABLE);

        table.put(new Put(Bytes.toBytes(ID))
                .addColumn(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(CELL_1), Bytes.toBytes("value 1"))
                .addColumn(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(CELL_2), Bytes.toBytes("value 1")));

        // 3. Do kolumny nr 1 dodać 6 kolejnych wpisów (wersji)
        for (int i = 2; i <= 7; i++) {
            table.put(new Put(Bytes.toBytes(ID))
                    .addColumn(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(CELL_1), Bytes.toBytes("value " + i)));
        }

        // 4. Usunąć najnowszą wersję z kolumny nr 1
        admin.flush(JAVA_TABLE);
        table.delete(new Delete(Bytes.toBytes(ID)).addColumn(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(CELL_1)));

        // 5. Wyświetlić wszystkie wersje w kolumnie nr 1
        Result result = table.get(new Get(Bytes.toBytes(ID)).setMaxVersions(10));
        List<Cell> columnCells = result.getColumnCells(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(CELL_1));
        for (Cell columnCell : columnCells) {
            String value = Bytes.toString(CellUtil.cloneValue(columnCell));
            System.out.println(value);
        }

        // end
        admin.close();
        table.close();
        connection.close();
    }

}
