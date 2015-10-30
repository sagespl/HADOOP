package pl.com.sages.hbase.api.dao;


import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import pl.com.sages.hbase.api.model.Movie;
import pl.com.sages.hbase.api.util.ConnectionHandler;

import java.io.IOException;

public class MovieDao {

    public static final TableName TABLE = TableName.valueOf("movies");
    public static final byte[] CF = Bytes.toBytes("movies");

    public static final byte[] TITLE = Bytes.toBytes("title");
    public static final byte[] GENRES = Bytes.toBytes("genres");

    public void save(Movie movie) throws IOException {
        save(movie.getMovieId(), movie.getTitle(), movie.getGenres());
    }

    public void save(int movieId, String title, String genres) throws IOException {
        Table movies = ConnectionHandler.getConnection().getTable(TABLE);

        Put put = new Put(Bytes.toBytes(movieId));
        put.addColumn(CF, TITLE, Bytes.toBytes(title));
        put.addColumn(CF, GENRES, Bytes.toBytes(genres));

        movies.put(put);
        movies.close();
    }

    public static Movie createMovie(Result result) {

        byte[] movieId = result.getRow();
        byte[] title = result.getValue(CF, TITLE);
        byte[] genres = result.getValue(CF, GENRES);

        return new Movie(Bytes.toInt(movieId), Bytes.toString(title), Bytes.toString(genres));
    }

}
