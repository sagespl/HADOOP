package pl.com.sages.hbase.api.dao;


import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import pl.com.sages.hbase.api.model.Tag;
import pl.com.sages.hbase.api.util.ConnectionHandler;

import java.io.IOException;

public class TagDao {

    public static final TableName TABLE = TableName.valueOf("ratings");
    public static final byte[] CF = Bytes.toBytes("ratings");

    public static final byte[] USER_ID = Bytes.toBytes("userId");
    public static final byte[] MOVIE_ID = Bytes.toBytes("movieId");
    public static final byte[] TAG = Bytes.toBytes("tag");

    public void save(Tag tag) throws IOException {
        save(tag.getUserId(), tag.getMovieId(), tag.getTag(), tag.getTimestamp());
    }

    public void save(int userId, int movieId, String tag, long timestamp) throws IOException {
        Table ratings = ConnectionHandler.getConnection().getTable(TABLE);

        Put put = new Put(Bytes.toBytes(timestamp));
        put.addColumn(CF, USER_ID, Bytes.toBytes(userId));
        put.addColumn(CF, MOVIE_ID, Bytes.toBytes(movieId));
        put.addColumn(CF, TAG, Bytes.toBytes(tag));

        ratings.put(put);
        ratings.close();
    }

    public static Tag createTag(Result result) {

        byte[] timestamp = result.getRow();
        byte[] userId = result.getValue(CF, USER_ID);
        byte[] movieId = result.getValue(CF, MOVIE_ID);
        byte[] tag = result.getValue(CF, TAG);

        return new Tag(Bytes.toInt(userId), Bytes.toInt(movieId), Bytes.toString(tag), Bytes.toLong(timestamp));
    }

}
