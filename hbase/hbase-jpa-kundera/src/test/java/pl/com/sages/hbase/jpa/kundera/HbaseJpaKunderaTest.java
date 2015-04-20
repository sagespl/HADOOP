package pl.com.sages.hbase.jpa.kundera;

import org.junit.Test;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class HbaseJpaKunderaTest {

    @Test
    public void shouldSaveEntity() throws Exception {
        //given

        User user = new User();
        user.setUserId("0001");
        user.setFirstName("John");
        user.setLastName("Smith");
        user.setCity("London");

        EntityManagerFactory emf = Persistence.createEntityManagerFactory("sages");
        EntityManager em = emf.createEntityManager();

        //when
        em.persist(user);
        em.close();
        emf.close();

        //then

    }

    @Test
    public void shouldFindEntity() throws Exception {
        //given

        User user = new User();
        user.setUserId("0002");
        user.setFirstName("Jan");
        user.setLastName("Kowalski");
        user.setCity("London");

        EntityManagerFactory emf = Persistence.createEntityManagerFactory("sages");
        EntityManager em = emf.createEntityManager();

        //when
        em.persist(user);
        User findedUser = em.find(User.class, "0002");
        em.close();
        emf.close();

        //then
        assertThat(findedUser).isNotNull();
        assertThat(findedUser.getFirstName()).isEqualTo("Jan");
        assertThat(findedUser.getLastName()).isEqualTo("Kowalski");
    }

    @Test
    public void shouldFindAllEntity() throws Exception {
        //given

        User user1 = new User();
        user1.setUserId("0003");
        user1.setFirstName("Michał");
        user1.setLastName("Kajko");
        user1.setCity("Milan");

        User user2 = new User();
        user2.setUserId("0004");
        user2.setFirstName("Paweł");
        user2.setLastName("Sienkiewicz");
        user2.setCity("Warsaw");

        EntityManagerFactory emf = Persistence.createEntityManagerFactory("sages");
        EntityManager em = emf.createEntityManager();

        //when
        em.persist(user1);
        em.persist(user2);

        Query q = em.createQuery("Select p from User p");
        List<User> results = q.getResultList();

        em.close();
        emf.close();

        //then
        assertThat(results).isNotNull();
        assertThat(results.size()).isGreaterThan(1);
    }

    @Test
    public void shouldSaveEntityWithAccount() throws Exception {
        //given

        User user = new User();
        user.setUserId("0005");
        user.setFirstName("Jan");
        user.setLastName("Kowalski");
        user.setCity("London");

        Account account = new Account();
        account.setAccountId(123L);
        account.setName("My account");
        account.setValid(true);

        user.setAccount(account);

        EntityManagerFactory emf = Persistence.createEntityManagerFactory("sages");
        EntityManager em = emf.createEntityManager();

        //when
        em.persist(user);

        //then
        User findedUser = em.find(User.class, "0005");
        em.close();
        emf.close();

        assertThat(findedUser).isNotNull();
        assertThat(findedUser.getFirstName()).isEqualTo("Jan");
        assertThat(findedUser.getLastName()).isEqualTo("Kowalski");
        assertThat(findedUser.getAccount().getName()).isEqualTo("My account");
    }

}
