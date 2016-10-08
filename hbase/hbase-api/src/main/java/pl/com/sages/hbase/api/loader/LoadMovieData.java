package pl.com.sages.hbase.api.loader;

import pl.com.sages.hbase.api.dao.MovieDao;
import pl.com.sages.hbase.api.util.HBaseUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class LoadMovieData {

    public static final String MOVIE_DATA = "/home/sages/dane/ml-10M100K/movies.dat";

    public static void main(String[] args) throws IOException {
        new LoadMovieData().run();
    }

    public void run() throws IOException {
        HBaseUtil.recreateTable(MovieDao.TABLE, MovieDao.CF);

        MovieDao movieDao = new MovieDao();

        // MovieID::Title::Genres
        BufferedReader br = new BufferedReader(new FileReader(new File(MOVIE_DATA)));
        String line = "";
        String delimeter = "::";

        int count = 0;
        while ((line = br.readLine()) != null) {

            String[] data = line.split(delimeter);
            int movieId = Integer.parseInt(data[0]);
            String title = data[1];
            String genres = data[2];

            count++;
            if (count % 1000 == 0) {
                System.out.println("Wczytano " + count + " wierszy");
            }

            movieDao.save(movieId, title, genres);
        }

        br.close();
    }


}
