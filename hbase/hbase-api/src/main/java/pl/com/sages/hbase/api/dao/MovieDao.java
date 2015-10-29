package pl.com.sages.hbase.api.dao;


import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import pl.com.sages.hbase.api.model.Movie;
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

    public Movie findById(String movieId) throws IOException {
        Table movies = ConnectionHandler.getConnection().getTable(TABLE);

        Get get = new Get(Bytes.toBytes(movieId));
        get.addFamily(CF);

        Result result = movies.get(get);

        if (result.isEmpty()) {
            return null;
        }

        Movie movie = createMovie(result);
        movies.close();
        return movie;
    }

    public void delete(Movie movie) throws IOException {
        delete(movie.getMovieId());
    }

    public void delete(int movieId) throws IOException {
        Table movies = ConnectionHandler.getConnection().getTable(TABLE);

        Delete delete = new Delete(Bytes.toBytes(movieId));
        movies.delete(delete);

        movies.close();
    }

    public List<Movie> findAll() throws IOException {
        Table movies = ConnectionHandler.getConnection().getTable(TABLE);

        Scan scan = new Scan();
        scan.addFamily(CF);

        ResultScanner results = movies.getScanner(scan);
        ArrayList<Movie> list = new ArrayList<>();
        for (Result result : results) {
            list.add(createMovie(result));
        }

        movies.close();
        return list;
    }

    public static Movie createMovie(Result result) {

        byte[] movieId = result.getRow();
        byte[] title = result.getValue(CF, TITLE);
        byte[] genres = result.getValue(CF, GENRES);

        return new Movie(Bytes.toInt(movieId), Bytes.toString(title), Bytes.toString(genres));
    }

}
