package pl.com.sages.hbase.api.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import pl.com.sages.hbase.api.loader.UserDataFactory;
import pl.com.sages.hadoop.data.model.movielens.User;
import pl.com.sages.hbase.api.util.HBaseUtil;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static pl.com.sages.hbase.api.util.HbaseConfigurationFactory.getConfiguration;

public class UsersDaoTestExternalTest {

    public static final String EMAIL = "jan@kowalski.pl";
    public static final String FORENAME = "Jan";
    public static final String SURNAME = "Kowalski";
    public static final String PASSWORD = "k12l3iu12313;k";

    private UsersDao usersDao;

    @Before
    public void before() throws IOException {
        Configuration configuration = getConfiguration();
        usersDao = new UsersDao();

        HBaseUtil.recreateTable(UsersDao.TABLE, Bytes.toString(UsersDao.CF));

        UserDataFactory.insertTestData();
    }

    @Test
    public void shouldSaveUser() throws Exception {
        //given
        User user = new User(FORENAME, SURNAME, EMAIL, PASSWORD);

        //when
        usersDao.save(user);

        //then
        User resultUser = usersDao.findByEmail(EMAIL);
        assertThat(resultUser).isNotNull();
        assertThat(resultUser.getEmail()).isEqualTo(EMAIL);
        assertThat(resultUser.getForename()).isEqualTo(FORENAME);
        assertThat(resultUser.getSurname()).isEqualTo(SURNAME);
        assertThat(resultUser.getPassword()).isEqualTo(PASSWORD);
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