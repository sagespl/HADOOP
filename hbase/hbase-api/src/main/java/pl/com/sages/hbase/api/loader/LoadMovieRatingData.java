package pl.com.sages.hbase.api.loader;

import pl.com.sages.hbase.api.dao.RatingDao;
import pl.com.sages.hbase.api.util.HBaseUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class LoadMovieRatingData {

    public static final String RATING_DATA = "/home/sages/dane/ml-10M100K/ratings.dat";

    public static void main(String[] args) throws IOException {
        new LoadMovieRatingData().run();
    }

    public void run() throws IOException {
        HBaseUtil.recreateTable(RatingDao.TABLE, RatingDao.CF);

        RatingDao ratingDao = new RatingDao();

        //UserID::MovieID::Rating::Timestamp
        BufferedReader br = new BufferedReader(new FileReader(new File(RATING_DATA)));
        String line = "";
        String delimeter = "::";

        int count = 0;
        while ((line = br.readLine()) != null) {

            String[] data = line.split(delimeter);
            int userId = Integer.parseInt(data[0]);
            int movieId = Integer.parseInt(data[1]);
            Double rating = Double.parseDouble(data[2]);
            int timestamp = Integer.parseInt(data[3]);

            count++;
            if (count % 1000 == 0) {
                System.out.println("Wczytano " + count + " wierszy");
            }

            if (count > 10000) {
                break; // wczytujemy tylko pierwsze 10 tys wierszy
            }

            ratingDao.save(userId, movieId, rating, timestamp);
        }

        br.close();
    }

}
