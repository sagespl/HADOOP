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

public class LoadMovieRatingData {

    public static final String RATING_DATA = "/home/sages/Sages/dane/ml-10M100K/ratings.dat";

    public static final String TABLE_NAME = "ratings";
    public static final String FAMILY_NAME = "ratings";
    public static final String MOVIE_ID = "movieId";
    public static final String RATING = "rating";

    public static void main(String[] args) throws IOException {
        new LoadMovieRatingData().run();
    }

    public void run() throws IOException {
        Configuration configuration = HBaseConfiguration.create();

        TableFactory.recreateTable(configuration, TABLE_NAME, FAMILY_NAME);

        // wrzucanie danych do HBase
        HTableInterface ratings = new HTable(configuration, TABLE_NAME);

        BufferedReader br = new BufferedReader(new FileReader(new File(RATING_DATA)));
        String line = "";
        String delimeter = "::";

        //UserID::MovieID::Rating::Timestamp
        int ratingId = 1;
        while ((line = br.readLine()) != null) {

            String[] movieData = line.split(delimeter);
            String userId = movieData[0];
            String movieId = movieData[1];
            Double rating = Double.parseDouble(movieData[2]);
            String timestamp = movieData[3];

//            System.out.println(ratingId + " -> " + userId + "::" + movieId + "::" + rating);
            if (ratingId % 1000 == 0) {
                System.out.println("Wczytano " + ratingId + " wierszy");
            }

            if (ratingId > 10000) {
                break; // wczytujemy tylko pierwsze 10 tys wierszy
            }

            Put put = new Put(Bytes.toBytes(ratingId++));
            put.add(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(MOVIE_ID), Bytes.toBytes(movieId));
            put.add(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(RATING), Bytes.toBytes(rating));

            ratings.put(put);
        }

        br.close();
    }

}
