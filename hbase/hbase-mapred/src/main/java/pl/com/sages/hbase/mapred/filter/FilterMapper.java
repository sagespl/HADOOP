package pl.com.sages.hbase.mapred.filter;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import pl.com.sages.hbase.api.model.Movie;

import java.io.IOException;

import static pl.com.sages.hbase.api.dao.MovieDao.createMovie;

public class FilterMapper extends TableMapper<ImmutableBytesWritable, Put> {

    public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        Movie movie = createMovie(value);

        if (movie.getGenres().contains("Comedy")) {
            Put put = new Put(key.get());
            Cell[] cells = value.rawCells();
            for (Cell cell : cells) {
                put.add(cell);
            }
            context.write(key, put);
        }
    }

}
