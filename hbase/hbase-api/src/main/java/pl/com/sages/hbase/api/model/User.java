package pl.com.sages.hbase.api.model;

import org.apache.hadoop.hbase.util.Bytes;

public class User {

    private String forename;
    private String surname;
    private String email;
    private String password;

    public User() {
    }

    public User(String forename, String surname, String email, String password) {
        this.forename = forename;
        this.surname = surname;
        this.email = email;
        this.password = password;
    }

    public User(byte[] forename, byte[] surname, byte[] email, byte[] password) {
        this(Bytes.toString(forename), Bytes.toString(surname), Bytes.toString(email), Bytes.toString(password));
    }

    public String getForename() {
        return forename;
    }

    public void setForename(String forename) {
        this.forename = forename;
    }

    public String getSurname() {
        return surname;
    }

    public void setSurname(String surname) {
        this.surname = surname;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public String toString() {
        return "User{" +
                "forename='" + forename + '\'' +
                ", surname='" + surname + '\'' +
                ", email='" + email + '\'' +
                ", password='" + password + '\'' +
                '}';
    }

}
