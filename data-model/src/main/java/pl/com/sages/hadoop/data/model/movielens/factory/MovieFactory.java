package pl.com.sages.hadoop.data.model.movielens.factory;

import pl.com.sages.hadoop.data.model.movielens.Movie;

public class MovieFactory {

    private static final String DELIMITER = "::";

    public static Movie create(String line) {
        String[] data = line.split(DELIMITER);

        int movieId = Integer.parseInt(data[0]);
        String title = data[1];
        String genres = data[2];

        return new Movie(movieId, title, genres);
    }

}
