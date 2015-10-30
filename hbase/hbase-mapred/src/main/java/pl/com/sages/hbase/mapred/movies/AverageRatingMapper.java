package pl.com.sages.hbase.mapred.movies;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import pl.com.sages.hbase.api.dao.RatingDao;
import pl.com.sages.hbase.api.model.Rating;

import java.io.IOException;

public class AverageRatingMapper extends TableMapper<IntWritable, DoubleWritable> {

    private final DoubleWritable ONE = new DoubleWritable(1);
    private IntWritable movieId = new IntWritable();

    public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {

        Rating rating = RatingDao.createRating(value);

        movieId.set(rating.getMovieId());
        ONE.set(rating.getRating());

        context.write(movieId, ONE);
    }

}