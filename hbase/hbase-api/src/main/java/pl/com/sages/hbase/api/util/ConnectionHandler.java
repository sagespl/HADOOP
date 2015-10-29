package pl.com.sages.hbase.api.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import static pl.com.sages.hbase.api.util.HbaseConfigurationFactory.getConfiguration;

public class ConnectionHandler {

    private static Connection connection = createConnection();

    public static Connection getConnection() {
        return connection;
    }

    private static Connection createConnection() {
        try {
            Configuration configuration = getConfiguration();
            Connection connection = ConnectionFactory.createConnection(configuration);
            return connection;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
