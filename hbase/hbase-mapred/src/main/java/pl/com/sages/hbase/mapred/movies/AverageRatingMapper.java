package pl.com.sages.hbase.mapred.movies;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import pl.com.sages.hadoop.data.model.movielens.Rating;
import pl.com.sages.hbase.api.dao.RatingDao;

import java.io.IOException;

public class AverageRatingMapper extends TableMapper<IntWritable, DoubleWritable> {

    private final DoubleWritable rating = new DoubleWritable(1);
    private final IntWritable movieId = new IntWritable();

    public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {

        Rating rating = RatingDao.createRating(value);

        movieId.set(rating.getMovieId());
        this.rating.set(rating.getRating());

        context.write(movieId, this.rating);
    }

}