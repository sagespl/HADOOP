package pl.com.sages.hbase.api.loader;

import pl.com.sages.hadoop.data.model.movielens.Movie;
import pl.com.sages.hadoop.data.model.movielens.factory.MovieFactory;
import pl.com.sages.hbase.api.dao.MovieDao;
import pl.com.sages.hbase.api.util.HBaseUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LoadMovieData extends HBaseLoader {

    private static final String MOVIE_DATA = System.getenv("HADOOP_DATA") + "/ml-10M100K/movies.dat";

    public static void main(String[] args) throws IOException {
        new LoadMovieData().load();
    }

    @Override
    public void load() {
        try {

            HBaseUtil.recreateTable(MovieDao.TABLE, MovieDao.CF);

            MovieDao movieDao = new MovieDao();

            // MovieID::Title::Genres
            BufferedReader br = new BufferedReader(new FileReader(new File(MOVIE_DATA)));
            String line;
            int count = 0;
            List<Movie> movies = new ArrayList<>(COMMIT);
            while ((line = br.readLine()) != null) {

                Movie movie = MovieFactory.create(line);
                movies.add(movie);

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

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}
