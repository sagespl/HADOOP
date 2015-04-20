package pl.com.sages.hbase.api.loader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class LoadMovieData {

    public static final String MOVIE_DATA = "/home/sages/Sages/dane/ml-10M100K/movies.dat";

    public static final String TABLE_NAME = "movies";
    public static final String FAMILY_NAME = "movies";
    public static final String GENRES = "genres";
    public static final String TITLE = "title";

    public static void main(String[] args) throws IOException {
        new LoadMovieData().run();
    }

    public void run() throws IOException {
        Configuration configuration = HBaseConfiguration.create();

        TableFactory.recreateTable(configuration, TABLE_NAME, FAMILY_NAME);

        // wrzucanie danych do HBase
        HTableInterface movies = new HTable(configuration, TABLE_NAME);

        BufferedReader br = new BufferedReader(new FileReader(new File(MOVIE_DATA)));
        String line = "";
        String delimeter = "::";

        // MovieID::Title::Genres
        int count = 0;
        while ((line = br.readLine()) != null) {

            String[] movieData = line.split(delimeter);
            String movieId = movieData[0];
            String title = movieData[1];
            String genres = movieData[2];

//            System.out.println(movieId + "::" + title + "::" + genres);
            count++;
            if (count % 1000 == 0) {
                System.out.println("Wczytano " + count + " wierszy");
            }

            Put put = new Put(Bytes.toBytes(movieId));
            put.add(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(TITLE), Bytes.toBytes(title));
            put.add(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(GENRES), Bytes.toBytes(genres));

            movies.put(put);
        }

        br.close();
    }


}
