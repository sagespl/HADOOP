package pl.com.sages.hbase.api.util;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseTableBuilder {

    private TableName tableName;
    private List<String> familyNames = new ArrayList<>();

    public HBaseTableBuilder withTable(TableName tableName) {
        this.tableName = tableName;
        return this;
    }

    public HBaseTableBuilder withTable(String namespace, String tableName) {
        this.tableName = TableName.valueOf(namespace, tableName);
        return this;
    }

    public HBaseTableBuilder withTable(String tableName) {
        this.tableName = TableName.valueOf(tableName);
        return this;
    }

    public HBaseTableBuilder withFamily(String familyName) {
        familyNames.add(familyName);
        return this;
    }

    public HBaseTableBuilder withFamily(byte[] familyName) {
        familyNames.add(Bytes.toString(familyName));
        return this;
    }

    public void build() {
        try {

            Admin admin = ConnectionHandler.getConnection().getAdmin();

            // recreating namespace
            String namespace = tableName.getNamespaceAsString();
            HBaseUtil.createNamespaceIfNotExists(namespace);

            if (admin.tableExists(tableName)) {
                if (!admin.isTableDisabled(tableName)) {
                    admin.disableTable(tableName);
                }
                admin.deleteTable(tableName);
            }

            HTableDescriptor table = new HTableDescriptor(tableName);

            for (String familyName : familyNames) {
                HColumnDescriptor columnFamily = new HColumnDescriptor(familyName);
                columnFamily.setMaxVersions(1);
                table.addFamily(columnFamily);
            }

            admin.createTable(table);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
