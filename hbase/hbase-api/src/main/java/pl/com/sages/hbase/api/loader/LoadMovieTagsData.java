package pl.com.sages.hbase.api.loader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static pl.com.sages.hbase.api.util.HbaseConfigurationFactory.getConfiguration;

public class LoadMovieTagsData {

    public static final String MOVIE_PATH = "/home/sages/Sages/dane/ml-10M100K/movies.dat";
    public static final String RATING_PATH = "/home/sages/Sages/dane/ml-10M100K/ratings.dat";
    public static final String TAG_PATH = "/home/sages/Sages/dane/ml-10M100K/tags.dat";
    private static final TableName TAGS = TableName.valueOf("tags");
    private static final String CF = "tags";

    public static void main(String[] args) throws IOException {

        System.out.println("MovieLens");

        // 1. stworzyc tabele
        Configuration configuration = getConfiguration();
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();

        if (admin.tableExists(TAGS)) {
            admin.disableTable(TAGS);
            admin.deleteTable(TAGS);
        }

        HTableDescriptor tableDescriptor = new HTableDescriptor(TAGS);
        HColumnDescriptor columnDescriptor = new HColumnDescriptor(CF);
        tableDescriptor.addFamily(columnDescriptor);
        admin.createTable(tableDescriptor);

        // 2. wczytaj pliki do tabel
        Table tagsTable = connection.getTable(TAGS);

        BufferedReader movieReader = new BufferedReader(new FileReader(new File(TAG_PATH)));

        String line;
        int count = 0;
        while ((line = movieReader.readLine()) != null) {

            if (count++ > 10000) {
                break;
            }

            System.out.println(count + " " + line);

            String[] split = line.split("::");
            String userId = split[0];
            String movieId = split[1];
            String tagId = split[2];
            long timestamp = Long.parseLong(split[3]);

            Put put = new Put(Bytes.toBytes(String.valueOf(count)));
            put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("userid"), timestamp, Bytes.toBytes(userId));
            put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("movieid"), timestamp, Bytes.toBytes(movieId));
            put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("tagid"), timestamp, Bytes.toBytes(tagId));

            tagsTable.put(put);
        }

        admin.close();
        tagsTable.close();
        connection.close();

    }

}
