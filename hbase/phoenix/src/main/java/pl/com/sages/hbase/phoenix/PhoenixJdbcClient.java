package pl.com.sages.hbase.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Phoenix JDBC Client
 */
public class PhoenixJdbcClient {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Connection conn;

        // Check if the driver is present, if not exit
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        } catch (ClassNotFoundException e) {
            System.out.println("Phoenix JDBC driver is not found.");
            return;
        }

        conn =  DriverManager.getConnection("jdbc:phoenix:hdpmaster1:/hbase-unsecure");
        System.out.println("Got Phoenix connection");

        // Table name
        final String tableName = String.format("my_table_%d", Thread.currentThread().getId());

        // Create table
        String query  = "DROP TABLE IF EXIST %s";
        conn.createStatement().execute(String.format(query, tableName));
        query = "CREATE TABLE %s "+
                "(count INT not null, " +
                "name CHAR(50) not null, " +
                "comment VARCHAR, " +
                "CONSTRAINT pk PRIMARY KEY(count, name)) SALT_BUCKETS=4";
        conn.createStatement().execute(String.format(query, tableName));
        conn.commit();

        // Insert some values
        query = "UPSERT INTO %s VALUES(10, 'Doe', 'Some comment'";
        conn.createStatement().executeUpdate(String.format(query, tableName));
        query = "UPSERT INTO %s VALUES(20, 'Smith', 'Some other comment'";
        conn.createStatement().executeUpdate(String.format(query, tableName));
        query = "UPSERT INTO %s(count, name) VALUES(30, 'Kowalsky')";
        conn.createStatement().executeUpdate(String.format(query, tableName));
        conn.commit();

        // Simple select
        query = "SELECT * FROM %s";
        ResultSet rst = conn.createStatement().executeQuery(String.format(query, tableName));
        while (rst.next()) {
            System.out.println(String.format("%d, %s, %s", rst.getInt(1), rst.getString(2), rst.getString(3)));
        }

        // Delete something
        query = "DELETE FROM %s WHERE name = 'Smith'";
        conn.createStatement().executeUpdate(String.format(query, tableName));


        // Simple select again
        query = "SELECT * FROM %s";
        rst = conn.createStatement().executeQuery(String.format(query, tableName));
        while (rst.next()) {
            System.out.println(String.format("%d, %s, %s", rst.getInt(1), rst.getString(2), rst.getString(3)));
        }
    }
}
