package pl.com.sages.hadoop.mapreduce.movies;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MoviesReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    private Text result = new Text();

    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String title = "";
        String tags = "";
        for (Text val : values) {
            String value = val.toString();
            if (value.contains("\"")) {
                title = value;
            } else {
                tags += "|" + value;
            }
        }
        result.set(title + " " + tags);
        context.write(key, result);
    }

}