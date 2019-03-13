package pl.com.sages.hadoop.mapreduce.movielens.moviefilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import pl.com.sages.hadoop.mapreduce.movielens.moviecount.MovieCountMapper;
import pl.com.sages.hadoop.mapreduce.movielens.moviewithtag.MovieMapper;
import pl.com.sages.hadoop.mapreduce.movielens.moviewithtag.MovieWithTagReducer;
import pl.com.sages.hadoop.mapreduce.movielens.moviewithtag.TagMapper;

import java.io.IOException;

/**
 * Created by radek on 20.01.17.
 */
public class MovieFilterRunner {

    public static void main(String[] args) throws Exception {

        Path inputPath = new Path(args[0]);
        System.out.println(inputPath);

        Path outputPath = new Path(args[2]);
        System.out.println(outputPath);

        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(outputPath, true);

        Job job = createJob(inputPath, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static Job createJob(Path inputPath, Path outputPath) throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "movie filter");
        job.setJarByClass(MovieWithTagReducer.class);

        job.setMapperClass(MovieFilterMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setNumReduceTasks(0);

        return job;
    }

}
