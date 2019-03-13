package pl.com.sages.hbase.api.util;

import io.vavr.control.Try;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.CompactionState;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class HBaseUtil {

    private static final Connection connection = ConnectionHandler.getConnection();

    @Deprecated
    public static void recreateTable(String tableName, String familyName) {
        recreateTable(TableName.valueOf(tableName), familyName);
    }

    @Deprecated
    public static void recreateTable(TableName tableName, byte[] familyName) {
        recreateTable(tableName, Bytes.toString(familyName));
    }

    @Deprecated
    public static void recreateTable(TableName tableName, String familyName) {
        new HBaseTableBuilder().withTable(tableName).withFamily(familyName).rebuild();
    }

    public static TableName getUserTableName(String tableName) {
        String userName = System.getProperty("user.name");
        return TableName.valueOf(userName, tableName);
    }

    public static void createNamespaceIfNotExists(final String namespace, Admin admin) throws IOException {
        createNamespaceIfNotExists(namespace);
    }

    public static void createNamespaceIfNotExists(final String namespace) throws IOException {
        final Admin admin = connection.getAdmin();
        NamespaceDescriptor namespaceDescriptor;
        if (!HBaseUtil.namespaceExists(namespace)) {
            namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(namespaceDescriptor);
        }
        admin.close();
    }

    public static boolean namespaceExists(final String namespace) throws IOException {
        final Admin admin = connection.getAdmin();
        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
            if (namespaceDescriptor.getName().equals(namespace)) {
                return true;
            }
        }
        admin.close();
        return false;
    }

    public static double getTableAverageLocality(TableName tableName) {
        return Try.of(() -> {

            final Admin admin = connection.getAdmin();
            final ClusterStatus clusterStatus = admin.getClusterStatus();

            int numberOfTableRegions = 0;
            double locality = 0;

            for (ServerName serverName : clusterStatus.getServers()) {
                final ServerLoad serverLoad = clusterStatus.getLoad(serverName);

                for (Map.Entry<byte[], RegionLoad> entry : serverLoad.getRegionsLoad().entrySet()) {
                    final RegionLoad regionLoad = entry.getValue();

                    String regionName = regionLoad.getNameAsString();

                    if (regionName.contains(tableName.getNameAsString() + ",")) {

                        numberOfTableRegions++;

                        float dataLocality = regionLoad.getDataLocality();
                        locality += dataLocality;

                    }

                }
            }

            admin.close();
            return locality / numberOfTableRegions;

        }).get();
    }

    public static void compactTable(TableName tableName) {
        Try.run(() -> {

            final Admin admin = connection.getAdmin();
            admin.majorCompact(tableName);
            admin.close();

        });
    }

    public static boolean isCompaction(TableName tableName) {
        return Try.of(() -> getCompaction(tableName) != CompactionState.NONE).get();
    }

    public static CompactionState getCompaction(TableName tableName) {
        return Try.of(() -> {

            final Admin admin = connection.getAdmin();
            CompactionState compactionState = admin.getCompactionState(tableName);
            admin.close();

            return compactionState;
        }).get();
    }

    private static boolean isTableEnabled(final TableName tableName) {
        return Try.of(() -> {

            final Admin admin = connection.getAdmin();
            boolean enabled = admin.isTableEnabled(tableName);
            admin.close();

            return enabled;
        }).get();
    }

    public static List<TableInfo> getTableInfos() {
        return Try.of(() -> {

            final Admin admin = connection.getAdmin();

            List<TableInfo> tables = Arrays.stream(admin.listTables())
                    .map(HTableDescriptor::getTableName)
                    .map(tableName -> {
                        TableInfo tableInfo = new TableInfo();
                        tableInfo.setName(tableName);
                        tableInfo.setAverageLocality(getTableAverageLocality(tableName));
                        tableInfo.setEnabled(isTableEnabled(tableName));
                        if (tableInfo.isEnabled()) {
                            tableInfo.setCompaction(getCompaction(tableName));
                        }
                        return tableInfo;
                    })
                    .collect(Collectors.toList());

            admin.close();

            return tables;
        }).get();
    }

    public List<String> listTablesNames() {
        return Try.of(() -> {

            final Admin admin = connection.getAdmin();
            List<String> tables = Arrays.stream(admin.listTables()).map(HTableDescriptor::getNameAsString).collect(Collectors.toList());
            admin.close();

            return tables;

        }).get();
    }

}
