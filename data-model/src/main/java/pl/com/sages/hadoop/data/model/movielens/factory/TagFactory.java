package pl.com.sages.hadoop.data.model.movielens.factory;

import pl.com.sages.hadoop.data.model.movielens.Tag;

/**
 * Created by radek on 20.01.17.
 */
public class TagFactory {

    private static final String DELIMITER = "::";

    public static Tag create(String line) {
        String[] movieData = line.split(DELIMITER);

        int userId = Integer.parseInt(movieData[0]);
        int movieId = Integer.parseInt(movieData[1]);
        String tag = movieData[2];
        int timestamp = Integer.parseInt(movieData[3]);

        return new Tag(userId, movieId, tag, timestamp);
    }

}
