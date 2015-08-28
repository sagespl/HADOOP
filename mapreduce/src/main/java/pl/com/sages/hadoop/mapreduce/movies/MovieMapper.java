package pl.com.sages.hadoop.mapreduce.movies;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MovieMapper extends Mapper<Object, Text, IntWritable, Text> {

    public static final String DELIMITER = "::";

    private IntWritable movieIdWritable = new IntWritable();
    private Text titleWritable = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] movieData = line.split(DELIMITER);
        String movieId = movieData[0];
        String title = movieData[1];

        movieIdWritable.set(Integer.parseInt(movieId));
        titleWritable.set("\"" + title + "\"");
        context.write(movieIdWritable, titleWritable);
    }

}
