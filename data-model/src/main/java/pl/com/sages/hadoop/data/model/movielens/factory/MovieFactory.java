package pl.com.sages.hadoop.data.model.movielens.factory;

import pl.com.sages.hadoop.data.model.movielens.Movie;

/**
 * Created by radek on 20.01.17.
 */
public class MovieFactory {

    private static final String DELIMITER = "::";

    public static Movie create(String line) {
        String[] movieData = line.split(DELIMITER);

        int movieId = Integer.parseInt(movieData[0]);
        String title = movieData[1];
        String genres = movieData[2];

        return new Movie(movieId, title, genres);
    }

}
