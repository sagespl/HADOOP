package pl.com.sages.hbase.api.loader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTablePool;
import pl.com.sages.hbase.api.dao.UsersDao;
import pl.com.sages.hbase.api.model.User;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import static pl.com.sages.hbase.api.conf.HbaseConfigurationFactory.getConfiguration;

public class UserDataFactory {

    public static void insertTestData() throws IOException {
        Configuration configuration = getConfiguration();
        HTablePool pool = new HTablePool(configuration, 10);
        UsersDao usersDao = new UsersDao(pool);

        InputStream inputStream = UserDataFactory.class.getClassLoader().getResourceAsStream("users/users.csv");

        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        String line = "";
        String delimeter = ";";

        while ((line = br.readLine()) != null) {

            String[] userData = line.split(delimeter);

            User user = new User();
            user.setForename(userData[0]);
            user.setSurname(userData[1]);
            user.setEmail(userData[2]);
            user.setPassword(userData[3]);

            usersDao.save(user);

        }

        br.close();
    }

}
