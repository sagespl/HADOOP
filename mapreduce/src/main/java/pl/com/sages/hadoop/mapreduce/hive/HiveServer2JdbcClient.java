package pl.com.sages.hadoop.mapreduce.hive;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

/**
 *  Hive Server 2 JDBC client
 */
public class HiveServer2JdbcClient {
    private final static String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private Connection connection = null;
    private Statement statement;

    public boolean hasDriver() {
        try {
            Class.forName(DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            return false;
        }
        return true;
    }

    public void connect(String url, String user, String password) throws SQLException {
        try {
            connection = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            connection = null;
            throw e;
        }
    }

    public void disconnect() throws SQLException {
        connection.close();
    }

    public boolean isConnected() throws SQLException {
        if (connection == null || connection.isClosed()) {
            return false;
        }
        return true;
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        if (statement == null) {
            if (connection == null || !isConnected()) {
                throw new SQLException("Not connected");
            }
            statement = connection.createStatement();
        }

        System.out.println("Running query: " + sql);
        return statement.executeQuery(sql);
    }

    public boolean execute(String sql) throws SQLException {
        if (statement == null) {
            if (connection == null || !isConnected()) {
                throw new SQLException("Not connected");
            }
            statement = connection.createStatement();
        }

        System.out.println("Running query: " + sql);
        return statement.execute(sql);
    }

    public void runTest() throws SQLException {
        if (!isConnected()) {
            throw new SQLException("Not connected");
        }

        connection.createStatement();

        String tableName = "testHiveDriverTable";

        execute("drop table if exists " + tableName);

        execute("create table " + tableName + " (key int, value string)");

        // show tables
        String sql = "show tables '" + tableName + "'";

        ResultSet result = executeQuery(sql);
        if (result.next()) {
            System.out.println(result.getString(1));
        }

        // describe table
        sql = "describe " + tableName;
        result = executeQuery(sql);
        while (result.next()) {
            System.out.println(result.getString(1) + "\t" + result.getString(2));
        }

        // load data into table
        // NOTE: filepath has to be local to the hive server
        // NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
        //String filepath = "output/houses-by-type-count/20150113013115/part-r-00000";
        //sql = "load data local inpath '" + filepath + "' into table " + tableName;
        //execute(sql);

        // Hive 0.14 and newer only!!
        sql = "insert into table " + tableName + " values (1, 'value1'), (2, 'value2')";
        execute(sql);

        // select * query
        sql = "select * from " + tableName;
        result = executeQuery(sql);
        while (result.next()) {
            System.out.println(String.valueOf(result.getInt(1)) + "\t" + result.getString(2));
        }

        // regular hive query
        sql = "select count(1) from " + tableName;
        result = executeQuery(sql);
        while (result.next()) {
            System.out.println(result.getString(1));
        }

    }

    public static void main(String[] args) throws SQLException {
        HiveServer2JdbcClient client = new HiveServer2JdbcClient();
        if (!client.hasDriver()){
            System.out.printf("Driver %s not present", client.DRIVER_NAME);
        }
        client.connect("jdbc:hive2://sandbox:10000/default;auth=noSasl", "hive", "");
        //client.connect("jdbc:hive2://", "hive", "");
        client.runTest();
        client.disconnect();
    }
}