package pl.com.sages.hbase.api.dao;


import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import pl.com.sages.hadoop.data.model.movielens.Rating;
import pl.com.sages.hbase.api.util.ConnectionHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RatingDao {

    public static final TableName TABLE = TableName.valueOf("ratings");
    public static final byte[] CF = Bytes.toBytes("ratings");

    public static final byte[] USER_ID = Bytes.toBytes("userId");
    public static final byte[] MOVIE_ID = Bytes.toBytes("movieId");
    public static final byte[] RATING = Bytes.toBytes("rating");

    public void save(Rating rating) throws IOException {
        save(rating);
    }

    public void save(List<Rating> ratings) throws IOException {
        Table table = ConnectionHandler.getConnection().getTable(TABLE);

        List<Put> puts = new ArrayList<>(ratings.size());
        for (Rating rating : ratings) {
            puts.add(createPut(rating));
        }

        table.put(puts);
    }

    public void save(int userId, int movieId, double rating, int timestamp) throws IOException {
        Table table = ConnectionHandler.getConnection().getTable(TABLE);

        Put put = createPut(userId, movieId, rating, timestamp);

        table.put(put);
        table.close();
    }

    private Put createPut(Rating rating) {
        return createPut(rating.getUserId(), rating.getMovieId(), rating.getRating(), rating.getTimestamp());
    }

    private Put createPut(int userId, int movieId, double rating, int timestamp) {
        Put put = new Put(Bytes.toBytes(timestamp));
        put.addColumn(CF, USER_ID, Bytes.toBytes(userId));
        put.addColumn(CF, MOVIE_ID, Bytes.toBytes(movieId));
        put.addColumn(CF, RATING, Bytes.toBytes(rating));
        return put;
    }

    public static Rating createRating(Result result) {

        byte[] timestamp = result.getRow();
        byte[] userId = result.getValue(CF, USER_ID);
        byte[] movieId = result.getValue(CF, MOVIE_ID);
        byte[] rating = result.getValue(CF, RATING);

        return new Rating(Bytes.toInt(userId), Bytes.toInt(movieId), Bytes.toDouble(rating), Bytes.toInt(timestamp));
    }

}
