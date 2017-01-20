package pl.com.sages.hadoop.mapreduce.movielens.moviecount;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import pl.com.sages.hadoop.data.model.movielens.Movie;
import pl.com.sages.hadoop.data.model.movielens.factory.MovieFactory;

import java.io.IOException;

import static pl.com.sages.hadoop.data.model.movielens.factory.MovieFactory.*;

/**
 * Created by radek on 20.01.17.
 */
public class MovieCountMapper extends Mapper<Object, Text, NullWritable, NullWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        context.getCounter(MovieCounter.MOVIE_NUMBER).increment(1);
    }

}
