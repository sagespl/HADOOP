package pl.com.sages.hadoop.mapreduce.movielens.moviecount;

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
import pl.com.sages.hadoop.mapreduce.movielens.moviewithtag.MovieMapper;
import pl.com.sages.hadoop.mapreduce.movielens.moviewithtag.MovieWithTagReducer;
import pl.com.sages.hadoop.mapreduce.movielens.moviewithtag.TagMapper;

import java.io.IOException;

/**
 * Created by radek on 20.01.17.
 */
public class MovieCountRunner {

    public static void main(String[] args) throws Exception {

        Path inputPath = new Path(args[0]);
        System.out.println(inputPath);

        Job job = createJob(inputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static Job createJob(Path inputPath) throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "movie count");
        job.setJarByClass(MovieWithTagReducer.class);

        job.setMapperClass(MovieCountMapper.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, inputPath);
        job.setOutputFormatClass(NullOutputFormat.class);

        return job;
    }

}
