package pl.com.sages.hbase.api.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import pl.com.sages.hbase.api.loader.TableFactory;
import pl.com.sages.hbase.api.loader.UserDataFactory;
import pl.com.sages.hbase.api.model.User;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static pl.com.sages.hbase.api.conf.HbaseConfigurationFactory.getConfiguration;

public class UsersDaoTest {

    public static final String EMAIL = "jan@kowalski.pl";
    public static final String FORENAME = "Jan";
    public static final String SURNAME = "Kowalski";
    public static final String PASSWORD = "k12l3iu12313;k";

    private UsersDao usersDao;

    @Before
    public void before() throws IOException {
        Configuration configuration = getConfiguration();
        HTablePool pool = new HTablePool(configuration, 10);
        usersDao = new UsersDao(pool);

        TableFactory.recreateTable(configuration, Bytes.toString(UsersDao.TABLE_NAME), Bytes.toString(UsersDao.FAMILY_NAME));

        UserDataFactory.insertTestData();
    }

    @Test
    public void shouldSaveUser() throws Exception {
        //given
        User user = new User(FORENAME, SURNAME, EMAIL, PASSWORD);

        //when
        usersDao.save(user);

        //then
        User findedUser = usersDao.findByEmail(EMAIL);
        assertThat(findedUser).isNotNull();
        assertThat(findedUser.getEmail()).isEqualTo(EMAIL);
        assertThat(findedUser.getForename()).isEqualTo(FORENAME);
        assertThat(findedUser.getSurname()).isEqualTo(SURNAME);
        assertThat(findedUser.getPassword()).isEqualTo(PASSWORD);
    }

    @Test
    public void shouldFindAllUser() throws Exception {
        //given
        usersDao.save(FORENAME, SURNAME, "1", PASSWORD);
        usersDao.save(FORENAME, SURNAME, "2", PASSWORD);
        usersDao.save(FORENAME, SURNAME, "3", PASSWORD);

        //when
        List<User> users = usersDao.findAll();

        //then
        assertThat(users).isNotNull();
        assertThat(users.size()).isGreaterThan(3);
    }

}