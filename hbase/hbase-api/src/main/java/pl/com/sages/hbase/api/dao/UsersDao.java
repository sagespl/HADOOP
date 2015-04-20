package pl.com.sages.hbase.api.dao;


import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import pl.com.sages.hbase.api.model.User;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UsersDao {

    public static final byte[] TABLE_NAME = Bytes.toBytes("users");
    public static final byte[] FAMILY_NAME = Bytes.toBytes("users");

    public static final byte[] FORENAME_COL = Bytes.toBytes("forename");
    public static final byte[] SURNAME_COL = Bytes.toBytes("surname");
    public static final byte[] PASSWORD_COL = Bytes.toBytes("password");

    private HTablePool pool;

    public UsersDao(HTablePool pool) {
        this.pool = pool;
    }

    public void save(User user) throws IOException {
        save(user.getForename(), user.getSurname(), user.getEmail(), user.getPassword());
    }

    public void save(String forename, String surname, String email, String password) throws IOException {

        HTableInterface users = pool.getTable(TABLE_NAME);

        Put put = new Put(Bytes.toBytes(email));
        put.add(FAMILY_NAME, FORENAME_COL, Bytes.toBytes(forename));
        put.add(FAMILY_NAME, SURNAME_COL, Bytes.toBytes(surname));
        put.add(FAMILY_NAME, PASSWORD_COL, Bytes.toBytes(password));

        users.put(put);
        users.close();
    }

    public User findByEmail(String email) throws IOException {
        HTableInterface users = pool.getTable(TABLE_NAME);

        Get get = new Get(Bytes.toBytes(email));
        get.addFamily(FAMILY_NAME);

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
        HTableInterface users = pool.getTable(TABLE_NAME);

        Delete delete = new Delete(Bytes.toBytes(email));
        users.delete(delete);

        users.close();
    }

    public List<User> findAll() throws IOException {
        HTableInterface users = pool.getTable(TABLE_NAME);

        Scan scan = new Scan();
        scan.addFamily(FAMILY_NAME);

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
        byte[] forename = result.getValue(FAMILY_NAME, FORENAME_COL);
        byte[] surname = result.getValue(FAMILY_NAME, SURNAME_COL);
        byte[] password = result.getValue(FAMILY_NAME, PASSWORD_COL);

        return new User(forename, surname, email, password);
    }

}
