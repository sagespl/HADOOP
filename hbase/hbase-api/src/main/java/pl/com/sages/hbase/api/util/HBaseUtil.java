package pl.com.sages.hbase.api.util;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public abstract class HBaseUtil {

    public static void recreateTable(String tableName, String familyName) throws IOException {
        recreateTable(TableName.valueOf(tableName), familyName);
    }

    public static void recreateTable(TableName tableName, byte[] familyName) throws IOException {
        recreateTable(tableName, Bytes.toString(familyName));
    }

    public static void recreateTable(TableName tableName, String familyName) throws IOException {
        Admin admin = ConnectionHandler.getConnection().getAdmin();

        if (admin.tableExists(tableName)) {
            if (!admin.isTableDisabled(tableName)) {
                admin.disableTable(tableName);
            }
            admin.deleteTable(tableName);
        }

        HTableDescriptor table = new HTableDescriptor(tableName);
        HColumnDescriptor columnFamily = new HColumnDescriptor(familyName);
        columnFamily.setMaxVersions(1);
        table.addFamily(columnFamily);

        admin.createTable(table);
    }

}
