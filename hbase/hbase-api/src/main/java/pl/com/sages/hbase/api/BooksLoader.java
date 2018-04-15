package pl.com.sages.hbase.api;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;

/**
 * Pobranie lektury:
 * scp -r login@hdp1:/tmp/lektury /home/users/login/bigdata
 * <p>
 * Usunięcie wpisu w known hosts (problem dostępu)
 * ssh-keygen -R hdp1
 */
public class BooksLoader {

    public static final String BOOKS = "/home/users/login/bigdata/lektury/lektury-all";
//    public static final String BOOKS = "/home/radek/Sages/dane/lektury/lektury-all";

    // psvm
    public static void main(String[] args) throws Exception {
        // wczytywanie lektur do bazy (lektury-all)
        // tabelka (name, content) z zbioru lektury-all
        File[] files = new File(BOOKS).listFiles();
        if (files != null) {
            for (File file : files) {
                String content = IOUtils.toString(new FileInputStream(file));
                System.out.println(content);
            }
        }
    }

}
