package pl.com.sages.hadoop.mapreduce.movielens.moviefilter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import pl.com.sages.hadoop.data.model.movielens.Movie;
import pl.com.sages.hadoop.data.model.movielens.factory.MovieFactory;

import java.io.IOException;

/**
 * Created by radek on 20.01.17.
 */
public class MovieFilterMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        Movie movie = MovieFactory.create(line);
        if (movie.getGenres().toLowerCase().contains("action")) {
            context.write(null, value);
        }
    }

}
