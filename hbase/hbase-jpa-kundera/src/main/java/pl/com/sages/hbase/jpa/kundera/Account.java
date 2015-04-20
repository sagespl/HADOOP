package pl.com.sages.hbase.jpa.kundera;

import javax.persistence.*;

@Entity
@Table(name = "account", schema = "jpa_users@sages")
public class Account {

    @Id
//    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long accountId;

    @Column(name = "name")
    private String name;

    @Column(name = "valid")
    private boolean valid;


    public Long getAccountId() {
        return accountId;
    }

    public void setAccountId(Long accountId) {
        this.accountId = accountId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

}