package pl.com.sages.hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import pl.com.sages.hbase.api.util.HBaseUtil;

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
        HBaseUtil.createNamespaceIfNotExists(TABLE_NAME.getNamespaceAsString(), admin);
        admin.createTable(new HTableDescriptor(TABLE_NAME)
                .addFamily(new HColumnDescriptor(FAMILY_NAME).setMaxVersions(10)));

        // 2. dodać do tabeli java dwa wiersze: dwie kolumny, każda dwie wartości
        Table table = connection.getTable(TABLE_NAME);

        table.put(new Put(toBytes("id"))
                .addColumn(toBytes(FAMILY_NAME), toBytes("kol1"), toBytes("wersja 1"))
                .addColumn(toBytes(FAMILY_NAME), toBytes("kol2"), toBytes("wersja 1")));
        table.put(new Put(toBytes("id"))
                .addColumn(toBytes(FAMILY_NAME), toBytes("kol1"), toBytes("wersja 2"))
                .addColumn(toBytes(FAMILY_NAME), toBytes("kol2"), toBytes("wersja 2")));

        // 3. pobrać dane z bazy i wyświetlić na konsoli (system out)
        // a) GET
        // b*) Scan
    }

}
