package pl.com.sages.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;

/**
 * Created by xwsmolak on 11/12/16.
 */
public class HdfsMain {

    public static final String HDFS_OUTPUT_PATH="/user/xwsmolak/java";
    public static final String LOCAL_FILE="/home/users/xwsmolak/lektury-one-file/lektury.txt";

    public static void main(String[] args) throws IOException {

        FileSystem fs;
        Configuration conf = new Configuration(false);
        conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));

        fs = FileSystem.get(conf);
        //0. usuwamy katalog /user/xwsmolak/java
        if (fs.delete(new Path(HDFS_OUTPUT_PATH), true)) System.out.println("usunalem");

        //1. tworzenie katalogu /user/xwsmolak/java
        if (fs.mkdirs(new Path(HDFS_OUTPUT_PATH))) System.out.println("utworzylem katalog");

        //2. cp /home/users/xwsmolak/lektury-one-file/lektury.txt /user/xwsmolak/java
        Path localpath = new Path(LOCAL_FILE);
        Path remotepath = new Path(HDFS_OUTPUT_PATH+"/lektury_remote.txt");
        fs.copyFromLocalFile(localpath,remotepath);
        if (fs.exists(remotepath)) System.out.println("plik lektury utworzony");

        //3. replikacja na poziom2
        FileStatus fileStatus = fs.getFileStatus(remotepath);
        System.out.println("obecna replikacja wynosi "+fileStatus.getReplication());
        if (fileStatus.getReplication()!=2) {
            if (fs.setReplication(remotepath, (short) 2))
                System.out.println("zmienilem replikacje");
        }else System.out.println("replikacja juz jest na poziomie2");
        fileStatus = fs.getFileStatus(remotepath);
        System.out.println("obecna replikacja wynosi "+fileStatus.getReplication());

        Path remotepathdir = new Path(HDFS_OUTPUT_PATH);
        if (!fs.isFile(remotepathdir)) {
            RemoteIterator<LocatedFileStatus> ls = fs.listFiles(remotepathdir, false);
            FileStatus status;
            while (ls.hasNext()) {
                status = ls.next();
                System.out.println(status.getPath().toString());
            }
        }
    }
}
