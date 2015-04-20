package org.datanucleus.samples.jdo.tutorial;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import pl.com.sages.hbase.api.dao.UsersDao;
import pl.com.sages.hbase.api.loader.TableFactory;
import pl.com.sages.hbase.api.loader.UserDataFactory;
import pl.com.sages.hbase.jdo.datanucleus.Inventory;
import pl.com.sages.hbase.jdo.datanucleus.Product;
import pl.com.sages.hbase.jdo.datanucleus.User;

import javax.jdo.*;
import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static pl.com.sages.hbase.api.conf.HbaseConfigurationFactory.getConfiguration;

public class HbaseDatanucleusJdoTest {

    @Before
    public void before() throws IOException {
        Configuration configuration = getConfiguration();
        TableFactory.recreateTable(configuration, Bytes.toString(UsersDao.TABLE_NAME), Bytes.toString(UsersDao.FAMILY_NAME));
        UserDataFactory.insertTestData();
        TableFactory.recreateTable(configuration, Inventory.INVETORY, Inventory.INVETORY);
        TableFactory.recreateTable(configuration, Product.PRODUCT, Product.PRODUCT);

        JDOEnhancer enhancer = JDOHelper.getEnhancer();
        enhancer.setVerbose(true);
        enhancer.addPersistenceUnit("Sages");
        enhancer.enhance();
    }

    @Test
    public void shouldSaveEntity() throws Exception {
        //given

        PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory("Sages");
        PersistenceManager pm = pmf.getPersistenceManager();

        Transaction tx = pm.currentTransaction();

        //when
        try {
            tx.begin();

            User user = new User();
            user.setBlob("kwjeow");
            user.setFirstName("Jan");
            user.setLastName("Kowalski");
            user.setId(System.currentTimeMillis());

            pm.makePersistent(user);
            tx.commit();

        } finally {
            if (tx.isActive()) {
                tx.rollback();
            }
            pm.close();
        }
        //then

    }

    @Test
    public void shouldGetUsers() throws Exception {
        //given

        PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory("Sages");
        PersistenceManager pm = pmf.getPersistenceManager();

        Transaction tx = pm.currentTransaction();
        try {

            tx.begin();

            User user = new User();
            user.setBlob("kwjeow");
            user.setFirstName("Jan");
            user.setLastName("Kowalski");
            user.setId(System.currentTimeMillis());

            pm.makePersistent(user);

            user = new User();
            user.setBlob("data");
            user.setFirstName("Michał");
            user.setLastName("Lewandowski");
            user.setId(System.currentTimeMillis());

            pm.makePersistent(user);

            tx.commit();

        } catch (Exception e) {
            e.printStackTrace();
        }
        //when

        Query q = pm.newQuery("SELECT FROM " + User.class.getName());
        List<User> users = (List) q.execute();

        //then
        assertThat(users.size()).isGreaterThan(1);
    }

    @Test
    public void shouldSaveEntityProduct() throws Exception {
        //given

        PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory("Sages");
        PersistenceManager pm = pmf.getPersistenceManager();

        //when
        Transaction tx = pm.currentTransaction();
        try {
            tx.begin();
            Inventory inv = new Inventory("My Inventory");
            Product product = new Product("Sony Discman", "A standard discman from Sony", 49.99);
            inv.getProducts().add(product);
            pm.makePersistent(inv);
            tx.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (tx.isActive()) {
                tx.rollback();
            }
            pm.close();
        }

        //then
    }

}
