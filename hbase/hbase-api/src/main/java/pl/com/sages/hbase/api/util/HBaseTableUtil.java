package pl.com.sages.hbase.api.util;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class HBaseTableUtil {

    public static int countNumberOfRows(TableName tableName) throws IOException {
        int count = 0;
        Table table = ConnectionHandler.getConnection().getTable(tableName);
        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            count++;
        }
        table.close();
        return count;
    }

}
