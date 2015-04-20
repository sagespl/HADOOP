package pl.com.sages.hbase.jdo.datanucleus;

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.PrimaryKey;

@PersistenceCapable(table = Product.PRODUCT, schema = Product.PRODUCT, catalog = Product.PRODUCT)
public class Product {

    public static final String PRODUCT = "product";

    @PrimaryKey
    long id;

    String name = null;
    String description = null;
    double price = 0.0;

    public Product(String name, String desc, double price) {
        this.name = name;
        this.description = desc;
        this.price = price;
    }

    public Product() {
    }

}