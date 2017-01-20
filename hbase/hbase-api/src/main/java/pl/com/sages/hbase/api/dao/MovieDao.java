package pl.com.sages.hbase.api.dao;


import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import pl.com.sages.hadoop.data.model.movielens.Movie;
import pl.com.sages.hbase.api.util.ConnectionHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MovieDao {

    public static final TableName TABLE = TableName.valueOf("movies");
    public static final byte[] CF = Bytes.toBytes("movies");

    public static final byte[] TITLE = Bytes.toBytes("title");
    public static final byte[] GENRES = Bytes.toBytes("genres");

    public void save(Movie movie) throws IOException {
        save(movie);
    }

    public void save(List<Movie> movies) throws IOException {
        Table table = ConnectionHandler.getConnection().getTable(TABLE);

        List<Put> puts = new ArrayList<>(movies.size());
        for (Movie movie : movies) {
            puts.add(createPut(movie));
        }

        table.put(puts);
    }

    public void save(int movieId, String title, String genres) throws IOException {
        Table table = ConnectionHandler.getConnection().getTable(TABLE);

        Put put = createPut(movieId, title, genres);

        table.put(put);
        table.close();
    }

    private Put createPut(Movie movie) {
        return createPut(movie.getMovieId(), movie.getTitle(), movie.getGenres());
    }

    private Put createPut(int movieId, String title, String genres) {
        Put put = new Put(Bytes.toBytes(movieId));
        put.addColumn(CF, TITLE, Bytes.toBytes(title));
        put.addColumn(CF, GENRES, Bytes.toBytes(genres));
        return put;
    }

    public static Movie createMovie(Result result) {

        byte[] movieId = result.getRow();
        byte[] title = result.getValue(CF, TITLE);
        byte[] genres = result.getValue(CF, GENRES);

        return new Movie(Bytes.toInt(movieId), Bytes.toString(title), Bytes.toString(genres));
    }

}
