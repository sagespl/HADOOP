package pl.com.sages.hbase.api.dao;


import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import pl.com.sages.hadoop.data.model.users.User;
import pl.com.sages.hbase.api.util.ConnectionHandler;
import pl.com.sages.hbase.api.util.HBaseUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UsersDao {

    public static final TableName TABLE = HBaseUtil.getUserTableName("users");
    public static final byte[] CF = Bytes.toBytes("users");

    public static final byte[] FORENAME = Bytes.toBytes("forename");
    public static final byte[] SURNAME = Bytes.toBytes("surname");
    public static final byte[] PASSWORD = Bytes.toBytes("password");

    public void save(User user) throws IOException {
        save(user.getForename(), user.getSurname(), user.getEmail(), user.getPassword());
    }

    public void save(String forename, String surname, String email, String password) throws IOException {
        Table users = ConnectionHandler.getConnection().getTable(TABLE);

        Put put = new Put(Bytes.toBytes(email));
        put.addColumn(CF, FORENAME, Bytes.toBytes(forename));
        put.addColumn(CF, SURNAME, Bytes.toBytes(surname));
        put.addColumn(CF, PASSWORD, Bytes.toBytes(password));

        users.put(put);
        users.close();
    }

    public User findByEmail(String email) throws IOException {
        Table users = ConnectionHandler.getConnection().getTable(TABLE);

        Get get = new Get(Bytes.toBytes(email));
        get.addFamily(CF);

        Result result = users.get(get);

        if (result.isEmpty()) {
            return null;
        }

        User user = createUser(result);
        users.close();
        return user;
    }

    public void delete(User user) throws IOException {
        deleteByEmail(user.getEmail());
    }

    public void deleteByEmail(String email) throws IOException {
        Table users = ConnectionHandler.getConnection().getTable(TABLE);

        Delete delete = new Delete(Bytes.toBytes(email));
        users.delete(delete);

        users.close();
    }

    public List<User> findAll() throws IOException {
        Table users = ConnectionHandler.getConnection().getTable(TABLE);

        Scan scan = new Scan();
        scan.addFamily(CF);

        ResultScanner results = users.getScanner(scan);
        ArrayList<User> list = new ArrayList<>();
        for (Result result : results) {
            list.add(createUser(result));
        }

        users.close();
        return list;
    }

    private User createUser(Result result) {

        byte[] email = result.getRow();
        byte[] forename = result.getValue(CF, FORENAME);
        byte[] surname = result.getValue(CF, SURNAME);
        byte[] password = result.getValue(CF, PASSWORD);

        return new User(Bytes.toString(forename), Bytes.toString(surname), Bytes.toString(email), Bytes.toString(password));
    }

}
