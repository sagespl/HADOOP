package pl.com.sages.hbase.phoenix;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class PhoenixJdbcClientTest {

    private String tableName = "phoenix_test_table";
    private Connection conn;

    @Before
    public void before() throws Exception {
        // wczytywanie parametrów
        Properties properties = new Properties();
        properties.load(this.getClass().getClassLoader().getResourceAsStream("phoenix.properties"));
        // tworzenie polaczenia
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(properties.getProperty("phoenix.connection"));
        // tworzenie tabeli
        String query = "CREATE TABLE %s " +
                "(count INTEGER not null, " +
                "name CHAR(50) not null, " +
                "comment VARCHAR " +
                "CONSTRAINT pk PRIMARY KEY(count, name))";
        conn.createStatement().execute(String.format(query, tableName));
        conn.commit();
    }

    @After
    public void after() throws SQLException {
        String query = "DROP TABLE %s";
        conn.createStatement().execute(String.format(query, tableName));
        conn.commit();
    }

    @Test
    public void shouldInsertAndQueryValuesFromTable() throws Exception {
        // given
        conn.createStatement().executeUpdate(String.format("UPSERT INTO %s(count, name) VALUES(1, 'Kowalski')", tableName));
        conn.createStatement().executeUpdate(String.format("UPSERT INTO %s(count, name) VALUES(2, 'Nowak')", tableName));
        conn.createStatement().executeUpdate(String.format("UPSERT INTO %s(count, name, comment) VALUES(3, 'Kochanowski', 'Comment')", tableName));
        conn.createStatement().executeUpdate(String.format("UPSERT INTO %s(count, name, comment) VALUES(4, 'Doe', 'Some comment')", tableName));
        conn.createStatement().executeUpdate(String.format("UPSERT INTO %s(count, name, comment) VALUES(5, 'Malinowski', 'Some other comment')", tableName));
        conn.commit();

        // when
        ResultSet rst = conn.createStatement().executeQuery(String.format("SELECT * FROM %s", tableName));

        // done
        int count = 0;
        while (rst.next()) {
            count++;
            System.out.println(String.format("%d, %s, %s", rst.getInt(1), rst.getString(2), rst.getString(3)));
        }
        assertThat(count).isEqualTo(5);
    }

}