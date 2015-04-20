package pl.com.sages.hbase.jdo.datanucleus;

import javax.jdo.annotations.*;

@PersistenceCapable(table = "users", schema = "users", catalog = "users")
@Extensions({
        @Extension(vendorName = "datanucleus", key = "hbase.columnFamily.meta.bloomFilter", value = "ROWKEY"),
        @Extension(vendorName = "datanucleus", key = "hbase.columnFamily.meta.inMemory", value = "true"),
        @Extension(vendorName = "datanucleus", key = "hbase.table.sanity.checks", value = "false"),
})
public class User {

    @PrimaryKey(column = "users", name = "users:id")
    private long id;

    @Column(name = "users:blob")
    private String blob;

    @Column(name = "users:firstName")
    private String firstName;

    @Column(name = "users:lastName")
    private String lastName;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getBlob() {
        return blob;
    }

    public void setBlob(String blob) {
        this.blob = blob;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

}