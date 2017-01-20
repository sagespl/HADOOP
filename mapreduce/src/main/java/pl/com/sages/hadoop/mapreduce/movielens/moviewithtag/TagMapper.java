package pl.com.sages.hadoop.mapreduce.movielens.moviewithtag;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import pl.com.sages.hadoop.data.model.movielens.Tag;
import pl.com.sages.hadoop.data.model.movielens.factory.TagFactory;

import java.io.IOException;

public class TagMapper extends Mapper<Object, Text, IntWritable, Text> {

    private IntWritable movieIdWritable = new IntWritable();
    private Text tagWritable = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        Tag tag = TagFactory.create(line);

        movieIdWritable.set(tag.getMovieId());
        tagWritable.set(tag.getTag());
        context.write(movieIdWritable, tagWritable);
    }

}