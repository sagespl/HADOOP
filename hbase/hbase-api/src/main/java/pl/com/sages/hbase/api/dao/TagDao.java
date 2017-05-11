package pl.com.sages.hbase.api.dao;


import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import pl.com.sages.hadoop.data.model.movielens.Tag;
import pl.com.sages.hbase.api.util.ConnectionHandler;
import pl.com.sages.hbase.api.util.HBaseUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TagDao {

    public static final TableName TABLE = HBaseUtil.getUserTableName("tags");
    public static final byte[] CF = Bytes.toBytes("tags");

    public static final byte[] USER_ID = Bytes.toBytes("userId");
    public static final byte[] MOVIE_ID = Bytes.toBytes("movieId");
    public static final byte[] TAG = Bytes.toBytes("tag");

    public void save(Tag tag) throws IOException {
        save(tag.getUserId(), tag.getMovieId(), tag.getTag(), tag.getTimestamp());
    }

    public void save(List<Tag> tags) throws IOException {
        Table table = ConnectionHandler.getConnection().getTable(TABLE);

        List<Put> puts = new ArrayList<>(tags.size());
        for (Tag tag : tags) {
            puts.add(createPut(tag));
        }

        table.put(puts);
    }

    public void save(int userId, int movieId, String tag, int timestamp) throws IOException {
        Table table = ConnectionHandler.getConnection().getTable(TABLE);

        Put put = createPut(userId, movieId, tag, timestamp);

        table.put(put);
        table.close();
    }

    private Put createPut(Tag tag) {
        return createPut(tag.getUserId(), tag.getMovieId(), tag.getTag(), tag.getTimestamp());
    }

    private Put createPut(int userId, int movieId, String tag, int timestamp) {
        Put put = new Put(Bytes.toBytes(timestamp));
        put.addColumn(CF, USER_ID, Bytes.toBytes(userId));
        put.addColumn(CF, MOVIE_ID, Bytes.toBytes(movieId));
        put.addColumn(CF, TAG, Bytes.toBytes(tag));
        return put;
    }

    public static Tag createTag(Result result) {

        byte[] timestamp = result.getRow();
        byte[] userId = result.getValue(CF, USER_ID);
        byte[] movieId = result.getValue(CF, MOVIE_ID);
        byte[] tag = result.getValue(CF, TAG);

        return new Tag(Bytes.toInt(userId), Bytes.toInt(movieId), Bytes.toString(tag), Bytes.toInt(timestamp));
    }

}
