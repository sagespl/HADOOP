package pl.com.sages.hbase.api.util;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public abstract class HBaseUtil {

    public static void recreateTable(String namespace, String tableName, String familyName) {
        recreateTable(TableName.valueOf(namespace, tableName), familyName);
    }

    public static void recreateTable(String tableName, String familyName) {
        recreateTable(TableName.valueOf(tableName), familyName);
    }

    public static void recreateTable(TableName tableName, byte[] familyName) {
        recreateTable(tableName, Bytes.toString(familyName));
    }

    public static void recreateTable(TableName tableName, String familyName) {
        try {

            Admin admin = ConnectionHandler.getConnection().getAdmin();

            // recreating namespace
            String namespace = tableName.getNamespaceAsString();
            NamespaceDescriptor namespaceDescriptor;
            if (!namespaceExists(namespace, admin)) {
                namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
                admin.createNamespace(namespaceDescriptor);
            }

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

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static TableName getUserTableName(String tableName) {
        String userName = System.getProperty("user.name");
        return TableName.valueOf(userName, tableName);
    }

    private static boolean namespaceExists(final String namespace, final Admin admin) throws IOException {
        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
            if (namespaceDescriptor.getName().equals(namespace)) {
                return true;
            }
        }
        return false;
    }

}
