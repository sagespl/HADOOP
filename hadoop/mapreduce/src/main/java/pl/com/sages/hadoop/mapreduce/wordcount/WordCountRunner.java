package pl.com.sages.hadoop.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountRunner {

    public static void main(String[] args) throws Exception {

        Path inputPath = new Path(args[0]);
        System.out.println(inputPath);

        Path outputPath = new Path(args[1]);
        System.out.println(outputPath);

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        fs.delete(outputPath, true);

        Job job = createJob(inputPath, outputPath, conf);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static Job createJob(final Path inputPath, final Path outputPath, final Configuration conf) throws IOException {
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCountRunner.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(10);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        return job;
    }

}
