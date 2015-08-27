package pl.com.sages.hadoop.mapreduce.invertedindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class InvertedIndexRunner {

    public static void main(String[] args) throws Exception {

        Path inputPath = new Path(args[0]);
        System.out.println(inputPath);

        Path outputPath = new Path(args[1]);
        System.out.println(outputPath);

        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(outputPath, true);

        Job job = createJob(inputPath, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static Job createJob(Path inputPath, Path outputPath) throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(InvertedIndexRunner.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndexReducer.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        return job;
    }

}
