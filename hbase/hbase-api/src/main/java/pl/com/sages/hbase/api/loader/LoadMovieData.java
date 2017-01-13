package pl.com.sages.hbase.api.loader;

import pl.com.sages.hbase.api.dao.MovieDao;
import pl.com.sages.hbase.api.model.Movie;
import pl.com.sages.hbase.api.util.HBaseUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LoadMovieData {

    public static final String MOVIE_DATA = "/home/radek/Sages/dane/ml-10M100K/movies.dat";
    public static final int COMMIT = 100;

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
        List<Movie> movies = new ArrayList<>(COMMIT);
        while ((line = br.readLine()) != null) {

            String[] data = line.split(delimeter);
            int movieId = Integer.parseInt(data[0]);
            String title = data[1];
            String genres = data[2];

            movies.add(new Movie(movieId, title, genres));

            count++;
            if (count % COMMIT == 0) {
                movieDao.save(movies);
                movies = new ArrayList<>(COMMIT);
                System.out.println("Wczytano " + count + " wierszy");
            }
        }

        movieDao.save(movies);
        System.out.println("Wczytano " + count + " wierszy");

        br.close();
    }


}
