package pl.com.sages.hadoop.mapreduce.movies;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MovieTagMapper extends Mapper<Object, Text, IntWritable, Text> {

    public static final String DELIMITER = "::";

    private IntWritable movieIdWritable = new IntWritable();
    private Text tagWritable = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] movieData = line.split(DELIMITER);
        String movieId = movieData[1];
        String tag = movieData[2];

        movieIdWritable.set(Integer.parseInt(movieId));
        tagWritable.set(tag);
        context.write(movieIdWritable, tagWritable);
    }

}
