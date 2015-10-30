package pl.com.sages.hbase.api.dao;


import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import pl.com.sages.hbase.api.model.Rating;
import pl.com.sages.hbase.api.util.ConnectionHandler;

import java.io.IOException;

public class RatingDao {

    public static final TableName TABLE = TableName.valueOf("ratings");
    public static final byte[] CF = Bytes.toBytes("ratings");

    public static final byte[] USER_ID = Bytes.toBytes("userId");
    public static final byte[] MOVIE_ID = Bytes.toBytes("movieId");
    public static final byte[] RATING = Bytes.toBytes("rating");

    public void save(Rating rating) throws IOException {
        save(rating.getUserId(), rating.getMovieId(), rating.getRating(), rating.getTimestamp());
    }

    public void save(int userId, int movieId, double rating, long timestamp) throws IOException {
        Table ratings = ConnectionHandler.getConnection().getTable(TABLE);

        Put put = new Put(Bytes.toBytes(timestamp));
        put.addColumn(CF, USER_ID, Bytes.toBytes(userId));
        put.addColumn(CF, MOVIE_ID, Bytes.toBytes(movieId));
        put.addColumn(CF, RATING, Bytes.toBytes(rating));

        ratings.put(put);
        ratings.close();
    }

    public static Rating createRating(Result result) {

        byte[] timestamp = result.getRow();
        byte[] userId = result.getValue(CF, USER_ID);
        byte[] movieId = result.getValue(CF, MOVIE_ID);
        byte[] rating = result.getValue(CF, RATING);

        return new Rating(Bytes.toInt(userId), Bytes.toInt(movieId), Bytes.toDouble(rating), Bytes.toLong(timestamp));
    }

}
