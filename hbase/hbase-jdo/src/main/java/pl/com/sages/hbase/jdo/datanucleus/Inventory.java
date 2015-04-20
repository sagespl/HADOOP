package pl.com.sages.hbase.jdo.datanucleus;


import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.PrimaryKey;
import java.util.HashSet;
import java.util.Set;

@PersistenceCapable(table = Inventory.INVETORY, schema = Inventory.INVETORY, catalog = Inventory.INVETORY)
public class Inventory {

    public static final String INVETORY = "inventory";

    @PrimaryKey
    String name = null;

    Set<Product> products = new HashSet<Product>();

    public Inventory(String name) {
        this.name = name;
    }

    public Inventory() {
    }

    public Set<Product> getProducts() {
        return products;
    }

}
