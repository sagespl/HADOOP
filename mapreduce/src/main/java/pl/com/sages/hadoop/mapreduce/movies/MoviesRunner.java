package pl.com.sages.hadoop.mapreduce.movies;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MoviesRunner {

    public static void main(String[] args) throws Exception {

        Path inputPath1 = new Path(args[0]);
        System.out.println(inputPath1);

        Path inputPath2 = new Path(args[1]);
        System.out.println(inputPath2);

        Path outputPath = new Path(args[2]);
        System.out.println(outputPath);

        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(outputPath, true);

        Job job = createJob(inputPath1, inputPath2, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static Job createJob(Path inputPath1, Path inputPath2, Path outputPath) throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "movies join");
        job.setJarByClass(MoviesReducer.class);
//        job.setNumReduceTasks(3);

//        job.setCombinerClass(MoviesReducer.class);
        job.setReducerClass(MoviesReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, inputPath1, TextInputFormat.class, MovieMapper.class);
        MultipleInputs.addInputPath(job, inputPath2, TextInputFormat.class, MovieTagMapper.class);
        FileOutputFormat.setOutputPath(job, outputPath);
        return job;
    }

}
