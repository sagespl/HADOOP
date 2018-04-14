package pl.com.sages.hbase.api.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseTableBuilder {

    private Connection connection;

    private TableName tableName;
    private List<String> families = new ArrayList<>();
    private Compression.Algorithm compressionType = Compression.Algorithm.GZ;

    public HBaseTableBuilder() {
        this(ConnectionHandler.getConnection());
    }

    public HBaseTableBuilder(Connection connection) {
        this.connection = connection;
    }

    public HBaseTableBuilder(Configuration configuration) {
        try {
            this.connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

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
        this.families.add(familyName);
        return this;
    }

    public HBaseTableBuilder withFamily(byte[] familyName) {
        this.families.add(Bytes.toString(familyName));
        return this;
    }

    public HBaseTableBuilder withCompressionType(final Compression.Algorithm compressionType) {
        this.compressionType = compressionType;
        return this;
    }

    public void build() {
        try {

            Admin admin = connection.getAdmin();

            // recreating namespace
            String namespace = tableName.getNamespaceAsString();
            NamespaceDescriptor namespaceDescriptor;
            if (!namespaceExists(namespace, admin)) {
                namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
                admin.createNamespace(namespaceDescriptor);
            }

            HTableDescriptor table = new HTableDescriptor(tableName);

            for (String family : families) {
                HColumnDescriptor columnFamily = new HColumnDescriptor(family);
                columnFamily.setMaxVersions(1);
                columnFamily.setCompressionType(compressionType);

                table.addFamily(columnFamily);
            }

            admin.createTable(table);
            admin.close();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void rebuild() {
        drop();
        build();
    }

    private void drop() {
        try {

            Admin admin = connection.getAdmin();

            if (admin.tableExists(tableName)) {
                if (!admin.isTableDisabled(tableName)) {
                    admin.disableTable(tableName);
                }
                admin.deleteTable(tableName);
            }

            admin.close();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private boolean namespaceExists(final String namespace, final Admin admin) throws IOException {
        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
            if (namespaceDescriptor.getName().equals(namespace)) {
                return true;
            }
        }
        return false;
    }

}
