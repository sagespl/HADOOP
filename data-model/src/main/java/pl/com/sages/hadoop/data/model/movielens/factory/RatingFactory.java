package pl.com.sages.hadoop.data.model.movielens.factory;

import pl.com.sages.hadoop.data.model.movielens.Rating;

public class RatingFactory {

    private static final String DELIMITER = "::";

    public static Rating create(String line) {
        String[] data = line.split(DELIMITER);

        int userId = Integer.parseInt(data[0]);
        int movieId = Integer.parseInt(data[1]);
        Double rating = Double.parseDouble(data[2]);
        int timestamp = Integer.parseInt(data[3]);

        return new Rating(userId, movieId, rating, timestamp);
    }

}
