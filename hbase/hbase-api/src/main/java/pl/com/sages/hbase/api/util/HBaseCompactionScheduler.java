package pl.com.sages.hbase.api.util;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.List;

public class HBaseCompactionScheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseCompactionScheduler.class);

    public void run() throws IOException {
        try {
            LOGGER.info("Starting compaction...");

            final Connection connection = ConnectionHandler.getConnection();

            List<TableInfo> tables = HBaseUtil.getTableInfos();

            tables.forEach(table -> LOGGER.info(MessageFormat.format("Tabela: {0}, aktywna: {1}, kompaktowanie: {2}, lokalność: {3}",
                    table.getName().getNameAsString(),
                    table.isEnabled(),
                    table.getCompaction(),
                    table.getAverageLocality()
            )));

            tables.stream()
                    .filter(t -> t.getAverageLocality() < 1 && t.getCompaction() != CompactionState.MAJOR && t.isEnabled())
                    .peek(t -> LOGGER.info("Compacting table: " + t.getName()))
                    .forEach(t -> HBaseUtil.compactTable(t.getName()));

            connection.close();
            LOGGER.info("Compaction started...");
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        new HBaseCompactionScheduler().run();
    }

}
