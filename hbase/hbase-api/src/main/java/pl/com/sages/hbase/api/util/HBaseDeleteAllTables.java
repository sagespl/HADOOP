package pl.com.sages.hbase.api.util;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.List;

public class HBaseDeleteAllTables {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseDeleteAllTables.class);

    public void run() throws IOException {
        try {
            LOGGER.info("Starting compaction...");

            final Connection connection = ConnectionHandler.getConnection();
            Admin admin = connection.getAdmin();

            List<TableInfo> tables = HBaseUtil.getTableInfos();

            tables.forEach(table -> {
                        LOGGER.info(MessageFormat.format("Tabela: {0}, aktywna: {1}, kompaktowanie: {2}, lokalność: {3}",
                                table.getName().getNameAsString(),
                                table.isEnabled(),
                                table.getCompaction(),
                                table.getAverageLocality()));
                        try {
                            if (admin.tableExists(table.getName())) {
                                admin.disableTable(table.getName());
                                admin.deleteTable(table.getName());
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
            );

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        new HBaseDeleteAllTables().run();
    }

}
